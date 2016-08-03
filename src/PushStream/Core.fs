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

type Completion   = bool                        -> unit
type Receiver<'T> = 'T                          -> bool
type Stream<'T>   = Receiver<'T> -> Completion  -> unit

module Stream =
  module Internals =
    let defaultSize       = 16
    let nopImpl more      = ()
    let nop : Completion  = nopImpl
    let inline adapt (s : Stream<'T>) = OptimizedClosures.FSharpFunc<_, _, _>.Adapt s

    module Loop =
      let rec ofArray (vs : 'T []) r i = if i < vs.Length then r vs.[i] && ofArray vs r (i + 1) else true
      let rec ofList r l =
        match l with
        | x::xs -> r x && ofList r xs
        | _     -> true
      let rec ofResizeArray (vs : ResizeArray<_>) r i = if i < vs.Count then r vs.[i] && ofResizeArray vs r (i + 1) else true
      let rec rangeForward s e r i = if i <= e then r i && rangeForward s e r (i + s) else true
      let rec rangeReverse s e r i = if i >= e then r i && rangeReverse s e r (i + s) else true
      let rec repeat v n r i = if i < n then r v && repeat v n r (i + 1) else true
      let rec unfold f r s =
        match f s with
        | Some (v, s) -> r v && unfold f r s
        | None        -> true

  open Internals
  open System

  // sources

  let empty<'T> : Stream<'T> =
    fun r c ->
      true |> c

  let inline ofArray (vs : 'T []) : Stream<'T> =
    fun r c ->
      Loop.ofArray vs r 0 |> c

  let inline ofList (vs : 'T list) : Stream<'T> =
    fun r c ->
      Loop.ofList r vs |> c

  let inline ofResizeArray (vs : ResizeArray<'T>) : Stream<'T> =
    fun r c ->
      Loop.ofResizeArray vs r 0 |> c

  let inline range b s e : Stream<'T> =
    if s = 0 then
      raise (ArgumentException ("Step of range can not be 0", "s"))
    elif b <= e && s > 0 then
      fun r c ->
        Loop.rangeForward s e r b |> c
    elif e <= b && s < 0 then
      fun r c ->
        Loop.rangeReverse s e r b |> c
    else
      empty

  let inline repeat v n : Stream<'T> =
    fun r c ->
      Loop.repeat v n r 0 |> c

  let inline singleton v : Stream<'T> =
    fun r c ->
      r v |> c

  let inline unfold (f : 'S -> ('T*'S) option) (z : 'S): Stream<'T> =
    fun r c ->
      Loop.unfold f r z |> c

  // pipes

  let inline choose (f : 'T -> 'U option) (s : Stream<'T>) : Stream<'U> =
    let s = adapt s
    fun r c ->
      s.Invoke ((fun v -> match f v with Some u -> r u | None -> true), c)

  let inline collect (m : 'T -> Stream<'U>) (s : Stream<'T>) : Stream<'U> =
    let s = adapt s
    fun r c ->
      let mutable cont = true
      let mc more = cont <- cont && more
      s.Invoke (
        (fun v ->
          let ms = m v
          let ms = adapt ms
          ms.Invoke (r, mc)
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
          c more
          printfn "STREAM: %s - more:%A" name more
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
    let oc = OptimizedClosures.FSharpFunc<_, _, _>.Adapt m
    fun r c ->
      let mutable i = -1
      s.Invoke ((fun v -> i <- i + 1; r (oc.Invoke (i, v))), c)

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
    let oc = OptimizedClosures.FSharpFunc<_, _, _>.Adapt f
    fun r c ->
      let mutable acc = z
      s.Invoke (
        (fun v ->
          match oc.Invoke (acc, v) with
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
        (fun more ->
          c more
          if more then
            ra.Sort comp
            let rec loop i = if i < ra.Count && r ra.[i] then loop (i + 1)
            Loop.ofResizeArray ra r 0 |> ignore
        ))

  let inline take n (s : Stream<'T>) : Stream<'T> =
    let s = adapt s
    fun r c ->
      let mutable rem = n
      s.Invoke ((fun v -> if rem > 0 then rem <- rem - 1; r v else false), c)

  let inline windowed size (s : Stream<'T>) : Stream<'T []> =
    let s = adapt s
    let size = max 1 size
    fun r c ->
      let ra = ResizeArray defaultSize
      s.Invoke (
        (fun v ->
        if ra.Count < size then
          ra.Add v
          true
        else
          let vs = ra.ToArray ()
          ra.Clear ()
          r vs
        ),
        (fun more ->
          if more && ra.Count > 0 then
            r (ra.ToArray ()) |> ignore
          c more
        ))

  // sinks

  let inline first dv (s : Stream<'T>) : 'T =
    let s = adapt s
    let mutable acc = dv
    s.Invoke ((fun v -> acc <- v; false), nop)
    acc

  let inline fold (f : 'S -> 'T -> 'S) (z : 'S) (s : Stream<'T>) : 'S =
    let s = adapt s
    let oc = OptimizedClosures.FSharpFunc<_, _, _>.Adapt f
    let mutable acc = z
    s.Invoke ((fun v -> acc <- oc.Invoke (acc, v); true), nop)
    acc

  let inline reduce (r : 'T -> 'T -> 'T) (s : Stream<'T>) : 'T =
    let s = adapt s
    let oc = OptimizedClosures.FSharpFunc<_, _, _>.Adapt r
    let mutable acc = LanguagePrimitives.GenericZero
    s.Invoke ((fun v -> acc <- oc.Invoke (acc, v); true), nop)
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
