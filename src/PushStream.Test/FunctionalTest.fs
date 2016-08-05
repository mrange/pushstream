// ----------------------------------------------------------------------------------------------
// Copyright 2016 Mårten Rånge
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
module FunctionalTest

open System
open System.Collections.Generic
open System.Linq

open FsCheck
open PushStream

let inline clamp v b e =
  if    v < b then b
  elif  e < v then e
  else  v

let between v b e =
  let bb = min b e
  let ee = max b e
  let dd = ee - bb
  if dd = 0 then bb
  else
    let aa = (v - bb) % dd |> abs
    bb + aa

type FilterOption =
  | All
  | Nothing
  | Mod2

type Between1And10 =
  | Between1And10 of int
  member x.Value =
    let (Between1And10 v) = x
    between v 1 10

type Between10And100 =
  | Between10And100 of int
  member x.Value =
    let (Between10And100 v) = x
    between v 10 100

type Chain =
  | ChunkBySize of Between1And10  // TODO: Make ChunkBySize recursive
  | DistinctBy
  | Map         of int
  | Skip        of Between1And10
  | Sort
  | Take        of Between10And100

type Properties() =

  static let chunkBySize sz_ (vs : 'T []) =
    let sz = clamp sz_ 1 Int32.MaxValue
    let ra = ResizeArray 16

    let rec loop i =
      if i + sz <= vs.Length then
        ra.Add vs.[i..(i + sz - 1)]
        loop (i + sz)
      elif i < vs.Length then
        ra.Add vs.[i..(vs.Length - 1)]
    loop 0

    ra.ToArray ()

  static let skip n (vs : #seq<'T>) = vs.Skip(n).ToArray()
  static let take n (vs : #seq<'T>) = vs.Take(n).ToArray()

  // utility
  static member ``test clamp`` (v : int) (x : int) (y : int) =
    let b = min x y
    let e = max x y
    let v = clamp v b e
    v >= b && v <= e

  static member ``test between`` (v : int) (x : int) (y : int) =
    let b = min x y
    let e = max x y
    let v = between v x y
    v >= b && v <= e

  static member ``test Between1And10`` (b : Between1And10) =
    let v = b.Value
    v >= 1 && v <= 10

  static member ``test Between10And100`` (b : Between10And100) =
    let v = b.Value
    v >= 10 && v <= 100

  // sources
  static member ``test empty`` () =
    let e = [||]
    let a = Stream.empty |> Stream.toArray
    e = a

  static member ``test ofArray`` (vs : int []) =
    let e = vs
    let a = vs |> Stream.ofArray |> Stream.toArray
    e = a

  static member ``test ofList`` (vs : int list) =
    let e = vs |> Array.ofList
    let a = vs |> Stream.ofList |> Stream.toArray
    e = a

  static member ``test ofResizeArray`` (vs : ResizeArray<int>) =
    let e = vs |> Array.ofSeq
    let a = vs |> Stream.ofResizeArray |> Stream.toArray
    e = a

  static member ``test ofSeq`` (vs : int []) =
    let e = vs
    let a = vs |> Stream.ofSeq |> Stream.toArray
    e = a

  static member ``test range`` (b : int) (s : int) (e : int) =
    let b = b % 100
    let s = s % 100
    let e = e % 100
    (s <> 0) ==> fun () ->
      let e_  = [| b..s..e |]
      let a   = Stream.range b s e |> Stream.toArray
      e_ = a

  static member ``test replicate`` (v : int64) (n : int) =
    let n = n % 100
    let e = [| for i in 1..n -> v |]
    let a = Stream.replicate n v |> Stream.toArray
    e = a

  static member ``test singleton`` (v : int) =
    let e = [|v|]
    let a = v |> Stream.singleton |> Stream.toArray
    e = a

  static member ``test unfold`` (l : int) =
    let l = l % 100
    let g s =
      if s < l then Some (int64 s, s + 1)
      else None
    let z   = 0
    let e = Seq.unfold g z |> Seq.toArray
    let a = Stream.unfold g z |> Stream.toArray
    e = a

  // pipes

  static member ``test choose`` (v : int) (fo : FilterOption) (vs : int []) =
    let c v =
      match fo with
      | All     -> Some (int64 v)
      | Nothing -> None
      | Mod2    -> if v % 2 = 0 then Some (int64 v) else None
    let e = vs |> Array.choose c
    let a = vs |> Stream.ofArray |> Stream.choose c |> Stream.toArray
    e = a

  static member ``test chunkBySize`` (sz : int) (vs : int []) =
    sz > 0 ==> fun () ->
      let e = vs |> chunkBySize sz
      let a = vs |> Stream.ofArray |> Stream.chunkBySize sz |> Stream.toArray
      e = a

  static member ``test collect`` (vs : int [] []) =
    let e = vs |> Array.collect id
    let a = vs |> Stream.ofArray |> Stream.collect Stream.ofArray |> Stream.toArray
    e = a

  static member ``test collect + take`` (n : int) (vs : int [] []) =
    let n = n % 5
    let e = vs |> Array.collect (take n)
    let a = vs |> Stream.ofArray |> Stream.collect (Stream.ofArray >> Stream.take n) |> Stream.toArray
    e = a

  static member ``test distinctBy`` (vs : int []) =
    let e = vs |> Seq.distinctBy int64 |> Seq.toArray
    let a = vs |> Stream.ofArray |> Stream.distinctBy int64 |> Stream.toArray
    e = a

  static member ``test exceptBy`` (f : int []) (s : int []) =
    let e = f.Except(s, Stream.Internals.equality int64).ToArray ()
    let a = f |> Stream.ofArray |> Stream.exceptBy int64 (s |> Stream.ofArray) |> Stream.toArray
    e = a

  static member ``test filter`` (fo : FilterOption) (vs : int []) =
    let f v =
      match fo with
      | All     -> true
      | Nothing -> false
      | Mod2    -> v % 2 = 0
    let e = vs |> Array.filter f
    let a = vs |> Stream.ofArray |> Stream.filter f |> Stream.toArray
    e = a

  static member ``test intersectBy`` (f : int []) (s : int []) =
    let e = f.Intersect(s, Stream.Internals.equality int64).ToArray ()
    let a = f |> Stream.ofArray |> Stream.intersectBy int64 (s |> Stream.ofArray) |> Stream.toArray
    e = a

  static member ``test map`` (i : int) (vs : int []) =
    let m = (+) i >> int64
    let e = vs |> Array.map m
    let a = vs |> Stream.ofArray |> Stream.map m |> Stream.toArray
    e = a

  static member ``test mapi`` (i : int) (vs : int []) =
    let m idx v = idx, v + i |> int64
    let e = vs |> Array.mapi m
    let a = vs |> Stream.ofArray |> Stream.mapi m |> Stream.toArray
    e = a

  static member ``test skip`` (s : int) (vs : int []) =
    let s = s % 10
    let e = vs |> skip s
    let a = vs |> Stream.ofArray |> Stream.skip s |> Stream.toArray
    e = a

  static member ``test sortBy`` (vs : int []) =
    let e = vs |> Array.sortBy id
    let a = vs |> Stream.ofArray |> Stream.sortBy id |> Stream.toArray
    e = a

  static member ``test take`` (t : int) (vs : int []) =
    let t = t % 10
    let e = vs |> take t
    let a = vs |> Stream.ofArray |> Stream.take t |> Stream.toArray
    e = a

  static member ``test unionBy`` (f : int []) (s : int []) =
    let e = f.Union(s, Stream.Internals.equality int64).ToArray ()
    let a = f |> Stream.ofArray |> Stream.unionBy int64 (s |> Stream.ofArray) |> Stream.toArray
    e = a

  // sinks
  static member ``test first`` (dv : int) (vs : int []) =
    let e = if vs.Length > 0 then vs.[0] else dv
    let a = vs |> Stream.ofArray |> Stream.first dv
    e = a

  static member ``test first - early return`` (dv : int) (vs : int []) =
    let mutable c = 0
    let e = if vs.Length > 0 then vs.[0] else dv
    let a = vs |> Stream.ofArray |> Stream.map (fun v -> c <- c + 1; v) |> Stream.first dv
    e = a && c = (min 1 vs.Length)

  static member ``test fold`` (vs : int []) =
    let f s v = s + int64 v
    let z = 0L
    let e = if vs.Length > 0 then vs |> Array.fold f z else z
    let a = vs |> Stream.ofArray |> Stream.fold f z
    e = a

  static member ``test reduce`` (vs : int []) =
    let r = (+)
    let e = if vs.Length > 0 then vs |> Array.reduce r else 0
    let a = vs |> Stream.ofArray |> Stream.reduce r
    e = a

  static member ``test sum`` (vs : int []) =
    let e = vs |> Array.sum
    let a = vs |> Stream.ofArray |> Stream.sum
    e = a

  static member ``test toArray`` (vs : int []) =
    let e = vs
    let a = vs |> Stream.ofArray |> Stream.toArray
    e = a

  static member ``test complex chain`` (vs : int []) =
    let f = fun v -> v % 2 = 0
    let m = (+) 1
    let e =
      vs
      |> Array.filter f
      |> chunkBySize 10
      |> Array.concat
      |> Array.sort
      |> Array.map m
    let a =
      vs
      |> Stream.ofArray
      |> Stream.filter f
      |> Stream.chunkBySize 10
      |> Stream.collect Stream.ofArray
      |> Stream.sortBy id
      |> Stream.map m
      |> Stream.toArray
    e = a

  static member ``test complex chains`` (chains : Chain []) (vs : int [])=
    let chains  = chains |> take 10
    let rec loop i e a =
      if i < chains.Length then
        let e, a =
          match chains.[i] with
          | ChunkBySize j -> e |> chunkBySize j.Value |> Array.concat , a |> Stream.chunkBySize j.Value |> Stream.collect Stream.ofArray
          | DistinctBy    -> e |> Seq.distinctBy int64 |> Seq.toArray , a |> Stream.distinctBy int64
          | Map         j -> e |> Array.map ((+) j)                   , a |> Stream.map ((+) j)
          | Skip        j -> e |> skip j.Value                        , a |> Stream.skip j.Value
          | Sort          -> e |> Array.sort                          , a |> Stream.sortBy id
          | Take        j -> e |> take j.Value                        , a |> Stream.take j.Value
        loop (i + 1) e a
      else e, a
    let se, sa = loop 0 vs (vs |> Stream.ofArray)
    let e = se
    let a = sa |> Stream.toArray
    e = a

let test () =
  // Code coverage for 'Stream.debug'
  Stream.range 4 -1 0
  |> Stream.debug "range"
  |> Stream.sortBy id
  |> Stream.debug "sortBy"
  |> Stream.sum
  |> ignore

#if DEBUG
  let maxTest = 1000
#else
  let maxTest = 1000
#endif

  let config = { Config.Quick with MaxTest = maxTest; MaxFail = maxTest }

  Check.All<Properties> config
