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
module TrivialStream

// The trivial stream is a very simplistic push stream that doesn't support
//  early exits (useful for first) or completion events (useful for sort)
//  The trivial stream is useful as basic stream to measure performance against

type Receiver<'T> = 'T            -> unit
type Stream<'T>   = Receiver<'T>  -> unit

module Details =
  module Loop =
    // This way to iterate seems to be faster in F#4 than a while loop
    let rec range s e r i = if i <= e then r i; range s e r (i + s)

open Details

// Sources

let inline range b s e : Stream<int> =
  fun r -> Loop.range s e r b

// Pipes

let inline filter (f : 'T -> bool) (s : Stream<'T>) : Stream<'T> =
  fun r -> s (fun v -> if f v then r v)

let inline map (m : 'T -> 'U) (s : Stream<'T>) : Stream<'U> =
  fun r -> s (fun v -> r (m v))

// Sinks

let inline sum (s : Stream<'T>) : 'T =
  let mutable ss = LanguagePrimitives.GenericZero
  s (fun v -> ss <- ss + v)
  ss
