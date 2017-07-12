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
module VariantStream

[<AbstractClass>]
type Receiver<'T>() =
  class
    abstract Receive: 'T -> unit
  end

[<AbstractClass>]
type Stream<'T>() =
  class
    abstract BuildUp: Receiver<'T> -> unit
  end

module Details =
  module Loop =
    // This way to iterate seems to be faster in F#4 than a while loop
    let rec range s e r i = if i <= e then (r : Receiver<_>).Receive i; range s e r (i + s)

open Details

// Sources

let inline range b s e : Stream<int> =
  { new Stream<int>() with
      override x.BuildUp r = Loop.range s e r b
  }

// Pipes

let inline filter (f : 'T -> bool) (s : Stream<'T>) : Stream<'T> =
  { new Stream<'T>() with
      override x.BuildUp r =
        s.BuildUp { new Receiver<'T>() with
          override x.Receive v = if f v then r.Receive v
        }
  }

let inline map (m : 'T -> 'U) (s : Stream<'T>) : Stream<'U> =
  { new Stream<'U>() with
      override x.BuildUp r =
        s.BuildUp { new Receiver<'T>() with
          override x.Receive v = r.Receive (m v)
        }
  }

// Sinks

let inline sum (s : Stream<'T>) : 'T =
  let mutable ss = LanguagePrimitives.GenericZero
  s.BuildUp { new Receiver<'T>() with
    override x.Receive v = ss <- ss + v
  }
  ss

