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
module TrivialStream

type Receiver<'T> = 'T            -> bool
type Stream<'T>   = Receiver<'T>  -> unit

module Details =
  module Loop =
    let rec ofArray (vs : 'T []) r i = if i < vs.Length then if r vs.[i] then ofArray vs r (i + 1)
    let rec range s e r i = if i <= e then if r i then range s e r (i + s)

open Details

// Sources

let inline ofArray (vs : 'T []) : Stream<'T> =
  fun r -> Loop.ofArray vs r 0

let inline range b s e : Stream<int> =
  fun r -> Loop.range s e r 0

// Pipes

let inline filter (f : 'T -> bool) (s : Stream<'T>) : Stream<'T> =
  fun r -> s (fun v -> if f v then r v else true)

let inline map (m : 'T -> 'U) (s : Stream<'T>) : Stream<'U> =
  fun r -> s (fun v -> r (m v))

let inline mapi (m : int -> 'T -> 'U) (s : Stream<'T>) : Stream<'U> =
  let om = OptimizedClosures.FSharpFunc<_, _, _>.Adapt m
  fun r ->
    let mutable i = -1
    s (fun v -> i <- i + 1; r (om.Invoke (i, v)))

// Sinks

let inline sum (s : Stream<'T>) : 'T =
  let mutable ss = LanguagePrimitives.GenericZero
  s (fun v -> ss <- ss + v; true)
  ss

let inline toArray (s : Stream<'T>) : 'T [] =
  let ra = ResizeArray 16
  s (fun v -> ra.Add v; true)
  ra.ToArray ()

