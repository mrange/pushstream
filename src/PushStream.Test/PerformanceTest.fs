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
module PerformanceTest

let now =
  let sw = System.Diagnostics.Stopwatch ()
  sw.Start ()
  fun () -> sw.ElapsedMilliseconds

let time n a =
  let inline cc i       = System.GC.CollectionCount i

  let v                 = a ()

  System.GC.Collect (2, System.GCCollectionMode.Forced, true)

  let bcc0, bcc1, bcc2  = cc 0, cc 1, cc 2
  let b                 = now ()

  for i in 1..n do
    a () |> ignore

  let e = now ()
  let ecc0, ecc1, ecc2  = cc 0, cc 1, cc 2

  v, (e - b), ecc0 - bcc0, ecc1 - bcc1, ecc2 - bcc2

open Nessos.Streams

let arrayTest n =
  Array.init (n + 1) id
  |> Array.map    int64
  |> Array.filter (fun v -> v % 2L = 0L)
  |> Array.map    ((+) 1L)
  |> Array.sum

let imperativeTest n =
  let rec loop s i =
    if i >= 0L then
      if i % 2L = 0L then
        loop (s + i + 1L) (i - 1L)
      else
        loop s (i - 1L)
    else
      s
  loop 0L (int64 n)

open System.Linq

let linqTest n =
  Enumerable.Range(0, n + 1).Select(int64).Where(fun v -> v % 2L = 0L).Select((+) 1L).Sum()

let nessosTest n =
  Stream.initInfinite int64
  |> Stream.take    (n + 1)
  |> Stream.filter  (fun v -> v % 2L = 0L)
  |> Stream.map     ((+) 1L)
  |> Stream.sum

open PushStream

let pushTest n =
  Stream.range 0 1 n
  |> Stream.map     int64
  |> Stream.filter  (fun v -> v % 2L = 0L)
  |> Stream.map     ((+) 1L)
  |> Stream.sum

let trivialTest n =
  TrivialStream.range 0 1 n
  |> TrivialStream.map     int64
  |> TrivialStream.filter  (fun v -> v % 2L = 0L)
  |> TrivialStream.map     ((+) 1L)
  |> TrivialStream.sum

open System.Diagnostics

let test (path : string) =
  let testCases =
    [|
      "imperative"  , imperativeTest  , false
      "trivialpush" , trivialTest     , false
      "pushstream"  , pushTest        , false
      "linq"        , linqTest        , false
      "nessos"      , nessosTest      , false
//      "array"       , arrayTest       , false
    |]
  use out                   = new System.IO.StreamWriter (path)
  let write (msg : string)  = out.WriteLine msg
  let writef fmt            = FSharp.Core.Printf.kprintf write fmt

  write "Name\tTotal\tOuter\tInner\tElapsed\tCC\tCC0\tCC1\tCC2\tResult"

  let total   = 100000000
  let outers =
    [|
      10        , false
      1000      , false
      1000000   , false
    |]
  for outer, obreak in outers do
    let inner = total / outer
    for name, a, ibreak in testCases do
      printfn "Running %s with total=%d, outer=%d, inner=%d ..." name total outer inner
      let v, ms, cc0, cc1, cc2 = time outer (fun () -> a inner)
      let cc = cc0 + cc1 + cc2
      printfn "  ... %d ms, cc=%d, cc0=%d, cc1=%d, cc2=%d, result=%A" ms cc cc0 cc1 cc2 v
      writef "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d" name total outer inner ms cc cc0 cc1 cc2 v
      if obreak && ibreak && Debugger.IsAttached then Debugger.Break ()
