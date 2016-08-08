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
module PushStream.Pipes

type Pipe<'T, 'TInput>  = Receiver<'T> -> Receiver<'TInput>

type Sink<'T, 'TInput>  = Receiver<'TInput>*(unit -> 'T)

[<RequireQualifiedAccess>]
module Pipe =
  module Internals =
    let defaultSize       = 16
    let inline adapt s    = OptimizedClosures.FSharpFunc<_, _, _>.Adapt s

    module Loop =
      let rec acceptArray (vs : 'T []) r i =
        if i < vs.Length then r vs.[i] && acceptArray vs r (i + 1) else true

  open Internals

  // sources

  [<GeneralizableValue>]
  let accept<'T> : Pipe<'T, 'T> =
    fun r v -> r v

  [<GeneralizableValue>]
  let acceptArray<'T> : Pipe<'T, 'T []> =
    fun r vs -> Loop.acceptArray vs r 0

  // pipes

  let inline filter (f : 'T -> bool) (p : Pipe<'T, 'TInput>) : Pipe<'T, 'TInput> =
    fun r -> p (fun v -> if f v then r v else true)

  let inline map (m : 'T -> 'U) (p : Pipe<'T, 'TInput>) : Pipe<'U, 'TInput> =
    fun r -> p (fun v -> r (m v))

  // sinks

  let inline toArray (p : Pipe<'T, 'TInput>) : Sink<'T [], 'TInput> =
    let ra = ResizeArray defaultSize
    p (fun v -> ra.Add v; true), fun () -> ra.ToArray ()

  let inline sum (p : Pipe<'T, 'TInput>) : Sink<'T, 'TInput> =
    let mutable acc = LanguagePrimitives.GenericZero
    p (fun v -> acc <- acc + v; true), fun () -> acc
