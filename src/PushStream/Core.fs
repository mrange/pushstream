// ----------------------------------------------------------------------------------------------
// Copyright 2015 Mårten Rånge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ----------------------------------------------------------------------------------------------
namespace PushStream

type Completion   = unit                        -> unit
type Receiver<'T> = 'T                          -> bool
type Stream<'T>   = Receiver<'T> -> Completion  -> unit

module Stream =
  module Internals =
    let defaultSize       = 16
    let nopImpl ()        = ()
    let nop : Completion  = nopImpl
    let inline adapt s    = OptimizedClosures.FSharpFunc<_, _, _>.Adapt s

    module Loop =
      // Function local loop functions currently make F# 4.0 create unnecessary loop objects
      //  By making loop functions external no unnecessary object is created
      let rec ofArray (vs : 'T []) r i = if i < vs.Length && r vs.[i] then ofArray vs r (i + 1)
      let rec ofList r l =
        match l with
        | x::xs when r x  -> ofList r xs
        | _               -> ()
      let rec ofResizeArray (vs : ResizeArray<_>) r i = if i < vs.Count && r vs.[i]then ofResizeArray vs r (i + 1)
      let rec rangeForward s e r i = if i <= e && r i then rangeForward s e r (i + s)
      let rec rangeReverse s e r i = if i >= e && r i then rangeReverse s e r (i + s)
      let rec repeat v n r i = if i < n && r v then repeat v n r (i + 1)
      let rec unfold f r s =
        match f s with
        | Some (v, s) when r v  -> unfold f r s
        | _                     -> ()

  open Internals
  open System

  // sources

  let empty<'T> : Stream<'T> =
    fun r c ->
      c ()

  let inline ofArray (vs : 'T []) : Stream<'T> =
    fun r c ->
      Loop.ofArray vs r 0
      c ()

  let inline ofList (vs : 'T list) : Stream<'T> =
    fun r c ->
      Loop.ofList r vs
      c ()

  let inline ofResizeArray (vs : ResizeArray<'T>) : Stream<'T> =
    fun r c ->
      Loop.ofResizeArray vs r 0
      c ()

  let inline range b s e : Stream<'T> =
    if s = 0 then
      raise (ArgumentException ("Step of range can not be 0", "s"))
    elif b <= e && s > 0 then
      fun r c ->
        Loop.rangeForward s e r b
        c ()
    elif e <= b && s < 0 then
      fun r c ->
        Loop.rangeReverse s e r b
        c ()
    else
      empty

  let inline repeat v n : Stream<'T> =
    fun r c ->
      Loop.repeat v n r 0
      c ()

  let inline singleton v : Stream<'T> =
    fun r c ->
      r v |> ignore
      c ()

  let inline unfold (f : 'S -> ('T*'S) option) (z : 'S): Stream<'T> =
    fun r c ->
      Loop.unfold f r z
      c ()

  // pipes

  let inline choose (f : 'T -> 'U option) (s : Stream<'T>) : Stream<'U> =
    let s = adapt s
    fun r c ->
      s.Invoke ((fun v -> match f v with Some u -> r u | None -> true), c)

  let inline chunkBySize size (s : Stream<'T>) : Stream<'T []> =
    let s = adapt s
    let size = max 1 size
    fun r c ->
      let ra = ResizeArray defaultSize
      s.Invoke (
        (fun v ->
          ra.Add v
          if ra.Count < size then
            true
          else
            let vs = ra.ToArray ()
            ra.Clear ()
            r vs
        ),
        (fun () ->
          if ra.Count > 0 then
            r (ra.ToArray ()) |> ignore
          c ()
        ))

  let inline collect (m : 'T -> Stream<'U>) (s : Stream<'T>) : Stream<'U> =
    let s = adapt s
    fun r c ->
      let mutable cont = true
      s.Invoke (
        (fun v ->
          let ms = m v
          let ms = adapt ms
          ms.Invoke ((fun v -> cont <- cont && r v; cont), nop)
          cont
        ),c)

  let inline debug name (s : Stream<'T>) : Stream<'T> =
    let s = adapt s
    fun r c ->
      s.Invoke (
        (fun v ->
          printfn "STREAM: %s - %A" name v
          r v),
        (fun more ->
          printfn "STREAM: %s - more:%A" name more
          c more
        ))

  let inline filter (f : 'T -> bool) (s : Stream<'T>) : Stream<'T> =
    let s = adapt s
    fun r c ->
      s.Invoke ((fun v -> if f v then r v else true), c)

  let inline map (m : 'T -> 'U) (s : Stream<'T>) : Stream<'U> =
    let s = adapt s
    fun r c ->
      s.Invoke ((fun v -> r (m v)), c)

  let inline mapi (m : int -> 'T -> 'U) (s : Stream<'T>) : Stream<'U> =
    let s = adapt s
    let m = adapt m
    fun r c ->
      let mutable i = -1
      s.Invoke ((fun v -> i <- i + 1; r (m.Invoke (i, v))), c)

  let inline skip n (s : Stream<'T>) : Stream<'T> =
    let s = adapt s
    fun r c ->
      let mutable rem = n
      s.Invoke ((fun v -> if rem > 0 then rem <- rem - 1; true else r v), c)

  type StreamingFold<'T, 'S> =
    | Stop
    | Fold    of 'S
    | Result  of 'T*'S

  let inline streamingFold (f : 'S -> 'T -> StreamingFold<'U, 'S>) (z : 'S) (s : Stream<'T>) : Stream<'U> =
    let s = adapt s
    let f = adapt f
    fun r c ->
      let mutable acc = z
      s.Invoke (
        (fun v ->
          match f.Invoke (acc, v) with
          | Stop          ->
            false
          | Fold s        ->
            acc <- s
            true
          | Result (u, s) ->
            acc <- s
            r u
        ), c)

  let inline sortBy (f : 'T -> 'U) (s : Stream<'T>) : Stream<'T> =
    let s = adapt s
    let comp = Comparison<'T> (fun l r ->
      let lv = f l
      let rv = f r
      compare lv rv
      )
    fun r c ->
      let ra = ResizeArray defaultSize
      s.Invoke (
        (fun v -> ra.Add v; true),
        (fun () ->
          ra.Sort comp
          Loop.ofResizeArray ra r 0 |> ignore
          c ()
        ))

  let inline take n (s : Stream<'T>) : Stream<'T> =
    let s = adapt s
    fun r c ->
      let mutable rem = n
      s.Invoke ((fun v -> if rem > 0 then rem <- rem - 1; r v else false), c)

  // sinks

  let inline first dv (s : Stream<'T>) : 'T =
    let s = adapt s
    let mutable acc = dv
    s.Invoke ((fun v -> acc <- v; false), nop)
    acc

  let inline fold (f : 'S -> 'T -> 'S) (z : 'S) (s : Stream<'T>) : 'S =
    let s = adapt s
    let f = adapt f
    let mutable acc = z
    s.Invoke ((fun v -> acc <- f.Invoke (acc, v); true), nop)
    acc

  let inline reduce (r : 'T -> 'T -> 'T) (s : Stream<'T>) : 'T =
    let s = adapt s
    let r = adapt r
    let mutable acc = LanguagePrimitives.GenericZero
    s.Invoke ((fun v -> acc <- r.Invoke (acc, v); true), nop)
    acc

  let inline sum (s : Stream<'T>) : 'T =
    let s = adapt s
    let mutable acc = LanguagePrimitives.GenericZero
    s.Invoke ((fun v -> acc <- acc + v; true), nop)
    acc

  let inline toArray (s : Stream<'T>) : 'T [] =
    let s = adapt s
    let ra = ResizeArray defaultSize
    s.Invoke ((fun v -> ra.Add v; true), nop)
    ra.ToArray ()

  // aliases

  let inline bind t uf = collect uf t
  let inline return_ v = singleton v
  let inline concat s  = collect id s
