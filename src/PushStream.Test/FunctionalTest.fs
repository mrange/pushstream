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
module FunctionalTest

open FsCheck
open PushStream

type FilterOption =
  | All
  | Nothing
  | Mod2

type Properties() =

  static let take n (vs : 'T []) = vs.[0..(min n (vs.Length - 1))]
  static let skip n (vs : 'T []) =
    if n < vs.Length then
      vs.[n..(vs.Length - n - 1)]
    else
      [| |]

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

  static member ``test range`` (b : int) (s : int) (e : int) =
    let b = b % 100
    let s = s % 100
    let e = e % 100
    (s <> 0) ==> fun () ->
      let e_  = [| b..s..e |]
      let a   = Stream.range b s e |> Stream.toArray
      e_ = a

  static member ``test repeat`` (v : int64) (n : int) =
    let e = [| for i in 1..n -> v |]
    let a = Stream.repeat v n |> Stream.toArray
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

  static member ``test collect`` (vs : int [] []) =
    let e = vs |> Array.collect id
    let a = vs |> Stream.ofArray |> Stream.collect Stream.ofArray |> Stream.toArray
    e = a

  static member ``test collect + take`` (n : int) (vs : int [] []) =
    let n = n % 5
    let e = vs |> Array.collect (take n)
    let a = vs |> Stream.ofArray |> Stream.collect (Stream.ofArray >> Stream.take n) |> Stream.toArray
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

// TODO: streamingFold

  static member ``test sortBy`` (vs : int []) =
    let e = vs |> Array.sortBy id
    let a = vs |> Stream.ofArray |> Stream.sortBy id |> Stream.toArray
    e = a

  static member ``test take`` (t : int) (vs : int []) =
    let t = t % 10
    let e = vs |> take t
    let a = vs |> Stream.ofArray |> Stream.take t |> Stream.toArray
    e = a

  static member ``test windowed`` (sz : int) (vs : int []) =
    sz > 0 ==> fun () ->
      let e = vs |> Seq.windowed sz |> Seq.toArray
      let a = vs |> Stream.ofArray |> Stream.windowed sz |> Stream.toArray
      e = a

  // sinks
  static member ``test first`` (dv : int) (vs : int []) =
    let e = if vs.Length > 0 then vs.[0] else dv
    let a = vs |> Stream.ofArray |> Stream.first dv
    e = a

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

  // TODO: Test complex chains with early returns
  //  Special care to chains containing multiple pipes that have completion actions

let test () =
  let config = Config.Quick
  Check.All<Properties> config
