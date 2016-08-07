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
module PushStream.Parallelism

open Pipes
open System.Threading.Tasks

type ParallelStream<'T, 'TInput> = Stream<'TInput>*Pipe<'T, 'TInput>

[<RequireQualifiedAccess>]
module Parallel =

  module Internals =
    open System
    open System.Collections.Generic
    open System.Threading

    type IOStatus =
      | Completed
      | Exception   of exn    // TODO: Handle multiple exceptions
      | Finished
      | Running     of int

    type PushPopResult<'T> =
      | Pushed
      | PushedAndPopped   of 'T
      | RejectedAndPopped of 'T
      | Rejected

    let inline isRunning s =
      match s with
      | Completed
      | Exception _
      | Finished    -> false
      | Running _   -> true

    type ConcurrentIO<'TInput, 'TOutput>(n : int) =
      let max             = 8*n
      let lockObject      = obj ()
      let input           = Queue<'TInput>  max
      let output          = Queue<'TOutput> max

      let mutable status  = Running n

      let attempt failWith action =
        lock lockObject (fun () ->
          let rec loop () =
            if isRunning status then
              match action () with
              | Some v  ->
                Monitor.Pulse lockObject
                v
              | None    ->
                Monitor.Wait lockObject |> ignore
                loop ()
            else
              failWith
          loop ()
          )

      let updateStatus u =
        lock lockObject (fun () ->
          match status with
          | Completed
          | Exception _
          | Finished    -> ()
          | Running   n ->
            status <- u n
            match status with
            | Completed
            | Exception _
            | Finished    -> Monitor.PulseAll lockObject
            | Running   _ -> ()
          )

      interface IDisposable with
        member x.Dispose () =
          x.Main_Finished ()
          input.Clear ()
          output.Clear ()

      member x.Main_Status : IOStatus =
        lock lockObject (fun _ -> status)

      member x.Shared_Exception (e : exn) : unit =
        updateStatus (fun _ -> Exception e)

      member x.Main_Finished () : unit =
        updateStatus (fun _ -> Finished)

      member x.Task_Completed () : unit =
        updateStatus (fun n -> if n > 1 then Running (n - 1) else Completed)

      member x.Task_TryPush (v : 'TOutput) : bool =
        let action () =
          if output.Count < max then
            output.Enqueue v
            true |> Some
          else
            None
        attempt false action

      member x.Task_TryPop () : 'TInput option =
        let action () =
          if input.Count > 0 then
            input.Dequeue () |> Some |> Some
          else
            None
        attempt None action

      member x.Main_TryPop () : 'TOutput option =
        let action () =
          if output.Count > 0 then
            output.Dequeue () |> Some |> Some
          else
            None
        attempt None action

      member x.Main_TryPushAndPop (v : 'TInput) : PushPopResult<'TOutput> =
        let action () =
          if input.Count < max then
            input.Enqueue v
            if output.Count > 0 then
              output.Dequeue () |> PushedAndPopped |> Some
            else
              Pushed |> Some
          else
            if output.Count > 0 then
              output.Dequeue () |> RejectedAndPopped |> Some
            else
              None
        attempt Rejected action

      member x.Main_PopRemaining () : 'TOutput [] =
        lock lockObject (fun () ->
          match status with
          | Completed   ->
            let vs = output.ToArray ()
            input.Clear ()
            output.Clear ()
            vs
          | Exception _
          | Finished
          | Running   _ -> failwith "Expected Completed status"
          )

  open Internals

  // sources

  let inline pfork (s : Stream<'T>) : ParallelStream<'T, 'T> =
    s, Pipe.accept

  // pipes

  let inline pfilter (f : 'T -> bool) ((s, p) : ParallelStream<'T, 'TInput>) : ParallelStream<'T, 'TInput> =
    s, Pipe.filter f p

  let inline pmap (m : 'T -> 'U) ((s, p) : ParallelStream<'T, 'TInput>) : ParallelStream<'U, 'TInput> =
    s, Pipe.map m p

  // sinks

  let pjoin (n : int) ((s, p) : ParallelStream<'T, 'TInput>) : Stream<'T> =
    if not (n > 0) then failwith "n expected to be greater than 0"
    fun r ->
      use io  = new ConcurrentIO<'TInput, 'T> (n)
      let pr  = p io.Task_TryPush
      let task () =
        try
          let rec loop () =
            match io.Task_TryPop () with
            | Some v  -> if pr v then loop ()
            | None    -> ()
          loop ()
          io.Task_Completed ()
        with
        | e -> io.Shared_Exception e

      let tasks =
        [|
          for i in 1..n do
            yield Task.Factory.StartNew(task, TaskCreationOptions.LongRunning)
        |]

      try
        try
          let rec popAndPushLoop input =
              match io.Main_TryPushAndPop input with
              | Pushed                    -> true
              | PushedAndPopped output    ->
                if r output then
                  true
                else
                  io.Main_Finished ()
                  false
              | RejectedAndPopped output  ->
                if r output then
                  popAndPushLoop input
                else
                  io.Main_Finished ()
                  false
              | Rejected                  -> false

          // Consumes the stream
          s popAndPushLoop

          // Wait for Tasks to complete their work
          match io.Main_Status with
          | Completed   -> ()
          | Running _   ->
            let rec popLoop () =
              match io.Main_TryPop () with
              | Some v  -> if r v then popLoop ()
              | None    -> ()
            popLoop ()
          | Exception e -> raise e
          | Finished    -> ()

          // Popping of the remaining items of the IO queue
          match io.Main_Status with
          | Completed   ->
            let vs = io.Main_PopRemaining ()
            let rec restLoop i =
              if i < vs.Length && r vs.[i] then restLoop (i + 1)
            restLoop 0
          | Running _   ->
            let e = System.InvalidOperationException "Running state is unexpected at this point"
            io.Shared_Exception e
            raise e
          | Exception e -> raise e
          | Finished    -> ()
        with
        | e ->
          io.Shared_Exception e
          reraise ()
      finally
        Task.WaitAll tasks
