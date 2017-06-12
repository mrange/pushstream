# pushstream

[![Build Status](https://travis-ci.org/mrange/pushstream.svg?branch=master)](https://travis-ci.org/mrange/pushstream)

`PushStream` is a simplistic but performant push data pipeline for F#

With the excellent data pipeline [Nessos Streams](https://github.com/nessos/Streams)
one can wonder why another data pipeline is needed.

`PushStream` do have some advantages over `Nessos Streams`

## Simpler implementation

Simplicity means fewer potential bugs and simpler to extend. Let's compare the
`singleton` implementation of `Nessos` and `PushStream`

### PushStream

```fsharp
let inline singleton v : Stream<'T> =
  fun r c ->
    r v |> ignore
```

### Nessos

```fsharp
let singleton (source: 'T) : Stream<'T> =
      Stream (fun { Complete = complete; Cont = iterf; Cts = cts }->
          let pulled = ref false
          { new Iterable with
              member __.Bulk() = iterf source; complete ()
              member __.Iterator =
                  { new Iterator with
                        member __.TryAdvance() =
                          if !pulled then false
                          else
                              iterf source
                              pulled := true
                              complete ()
                              true
                        member __.Dispose() = ()} })
```

`Nessos` uses a more complex definition because it supports pull & push data pipeline.
`PushStream` is strictly a push data pipeline.

## Lower overhead

Because of simpler definition `PushStream` has lower GC overhead which gives performance
benefits.

Let's compare some data pipelines:

  1. Imperative (not really a data pipeline but useful for sanity check)
  2. TrivialPush (a reference push stream that can only support the most basic features of data pipelines)
  3. PushStream
  4. Nessos
  5. LINQ (Why not F# Seq? Because Seq overhead is significantly higher than LINQ although both are based around `IEnumerable<>`)

We use a simple data pipe line to measure overhead:

```fsharp
let pushTest n =
  Stream.range 0 1 n
  |> Stream.map     int64
  |> Stream.filter  (fun v -> v % 2L = 0L)
  |> Stream.map     ((+) 1L)
  |> Stream.sum
```

An equivalent data pipeline is implemented for each alternative.

### Elapsed ms - F# 3.1 - .NET 4.5.2 - x64

[![Elapsed ms - F# 3.1 - .NET 4.5.2 - x64][1]][1]

The bars show the elapsed time in milliseconds. Lower is better.

The performance run is designed so that the total amount of useful work is the same
so that the numbers can be compared. A greater amount of test runs there for implies
smaller data sets.

We see `Nessos` and `PushStream` perform comparable until the streams start becoming
quite small (~10 elements). The larger overhead of `Nessos` streams then pushes up
the CPU time needed.

### Collection Count - F# 3.1 - .NET 4.5.2 - x64

[![Collection Count - F# 3.1 - .NET 4.5.2 - x64][2]][2]

The bars show the total count of GC collection runs during test runs. Lower is better.

We see that when running a large of amount of test runs that `PushStream` has a lower
GC overhead than `Nessos`.

## Conclusion

`Nessos` is a great data pipeline library but `PushStream` offers simplicity and lower
overhead if you don't need all features of `Nessos`.

## TODO

  1. Unsure on the ordering of arguments for append/unionBy etc. On one hand I read this: `let a = f |> Stream.append s` as `s` is append to `f`. OTOH `let a = Stream.append s f` reads as `f` is appended to `s`.

  [1]: img/perf_cpu.png
  [2]: img/perf_cc.png
