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
using System;
using System.Runtime.CompilerServices;

namespace CsStream
{
  public abstract class Receiver<T>
  {
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public abstract void Receive(T v);
  }

  public abstract class Stream<T>
  {
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public abstract void BuildUp(Receiver<T> r);
  }

  public static class Stream
  {
    public sealed class RangeStream : Stream<int>
    {
      readonly int b;
      readonly int s;
      readonly int e;

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public RangeStream(int b, int s, int e)
      {
        this.b = b;
        this.s = Math.Max(s, 1);
        this.e = e;
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override void BuildUp(Receiver<int> r)
      {
        for (var i = b; i <= e; i += s)
        {
          r.Receive(i);
        }
      }
    }

    public sealed class WhereReceiver<T> : Receiver<T>
    {
      readonly Receiver<T>    r;
      readonly Func<T, bool>  f;

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public WhereReceiver(Receiver<T> r, Func<T, bool> f)
      {
        this.r = r;
        this.f = f;
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override void Receive(T v)
      {
        if (f(v)) r.Receive(v);
      }
    }

    public sealed class WhereStream<T> : Stream<T>
    {
      readonly Stream<T>      s;
      readonly Func<T, bool>  f;

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public WhereStream(Stream<T> s, Func<T, bool> f)
      {
        this.s = s;
        this.f = f;
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override void BuildUp(Receiver<T> r)
      {
        s.BuildUp(new WhereReceiver<T>(r, f));
      }
    }

    public sealed class SelectReceiver<T, U> : Receiver<T>
    {
      readonly Receiver<U>    r;
      readonly Func<T, U>     f;

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public SelectReceiver(Receiver<U> r, Func<T, U> f)
      {
        this.r = r;
        this.f = f;
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override void Receive(T v)
      {
        r.Receive(f(v));
      }
    }

    public sealed class SelectStream<T, U> : Stream<U>
    {
      readonly Stream<T>      s;
      readonly Func<T, U>     f;

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public SelectStream(Stream<T> s, Func<T, U> f)
      {
        this.s = s;
        this.f = f;
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override void BuildUp(Receiver<U> r)
      {
        s.BuildUp(new SelectReceiver<T, U>(r, f));
      }
    }

    public sealed class SumReceiver : Receiver<long>
    {
      public long Sum = 0L;

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public override void Receive(long v)
      {
        Sum += v;
      }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static RangeStream Range(int b, int s, int e)
    {
      return new RangeStream(b, s, e);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static WhereStream<T> Where<T>(this Stream<T> s, Func<T, bool> f)
    {
      return new WhereStream<T>(s, f);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SelectStream<T, U> Select<T, U>(this Stream<T> s, Func<T, U> f)
    {
      return new SelectStream<T, U>(s, f);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Sum(this Stream<long> s)
    {
      var r = new SumReceiver();

      s.BuildUp(r);

      return r.Sum;
    }

    public static long TestIt(int n)
    {
      return Range(0, 1, n).Select(v => (long)v).Where(v => v % 2L == 0L).Select(v => v + 1L).Sum();
    }

  }

}
