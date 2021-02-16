using FASTER.core;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;



namespace ConsoleApp2
{

    [Serializable]
    class MyType
    {
        public int i;

        public override int GetHashCode()
        {
            return i.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if(!(obj is MyType))
            {
                return false;
            }

            return i.Equals(((MyType)obj).i);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var x = new d<MyType, MyType>();

            for (int i = 0; i < 1000000; i++)
            //Parallel.For(0, 1000000, i =>
            {
                x.Add(new MyType() { i = i }, new MyType() { i = i });
            }//);

            //x.Flush();

            for (int i = 0; i < 1000000; i++)
            {
                if (x[new MyType() { i = i }].i != i)
                {
                    throw new Exception("mismatch!");
                }
            }
            x.Dispose();
            Console.WriteLine(sw.Elapsed);
        }

        class d<KEY, VALUE> : IDictionary<KEY, VALUE>, IDisposable
        {

            VALUE context;
            private IDevice log;
            private IDevice objlog;
            private SerializerSettings<KEY, VALUE> serilizerSettings;
            private FasterKV<KEY, VALUE> store;
            private ClientSession<KEY, VALUE, VALUE, VALUE, Empty, IFunctions<KEY, VALUE, VALUE, VALUE, Empty>> s;

            //private ClientSession<KEY, VALUE, Empty, VALUE, Empty, IFunctions<KEY, VALUE, Empty, VALUE, Empty>> s;
            private bool disposed;

            public d()
            {
                log = Devices.CreateLogDevice(@"C:\Users\merlynop\source\repos\ConsoleApp2\bin\Debug\hlog.log");
                objlog = Devices.CreateLogDevice(@"C:\Users\merlynop\source\repos\ConsoleApp2\bin\Debug\hlog.obj.log");

                serilizerSettings = new SerializerSettings<KEY, VALUE>
                {
                    keySerializer = () => new MyValueSerializer<KEY>(),
                    valueSerializer = () => new MyValueSerializer<VALUE>()
                };

                store = new FasterKV<KEY, VALUE>(
                    1L << 20,
                    new LogSettings { LogDevice = log, ObjectLogDevice = objlog },
                    serializerSettings: serilizerSettings);

                //s = store.NewSession(new MyFunctions<KEY, VALUE>());
                s = store.NewSession(new SimpleFunctions<KEY, VALUE>());
            }

            public void Dispose()
            {
                if (!disposed)
                {

                    store.Log.FlushAndEvict(true);

                    // end session
                    s.Dispose();
                    // dispose store
                    store.Dispose();
                    // close logs
                    log.Dispose();
                    objlog.Dispose();

                    disposed = true;
                }
            }


            public VALUE this[KEY key]
            {
                get
                {
                    VALUE ret = default;

                    TryGetValue(key, out ret);

                    return ret;
                }


                set
                {
                    var r = s.Upsert(ref key, ref value);

                    if(r!= Status.OK)
                    {
                        throw new Exception("not okay");
                    }
                }
            }

            public ICollection<KEY> Keys => throw new NotImplementedException();

            public ICollection<VALUE> Values => throw new NotImplementedException();

            public int Count => throw new NotImplementedException();

            public bool IsReadOnly => false;

            public void Add(KEY key, VALUE value)
            {
                this[key] = value;
            }

            public void Add(KeyValuePair<KEY, VALUE> item)
            {
                this[item.Key] = item.Value;
            }

            public void Clear()
            {
                throw new NotImplementedException();
            }

            public bool Contains(KeyValuePair<KEY, VALUE> item)
            {
                throw new NotImplementedException();
            }

            public bool ContainsKey(KEY key)
            {
                throw new NotImplementedException();
            }

            public void CopyTo(KeyValuePair<KEY, VALUE>[] array, int arrayIndex)
            {
                throw new NotImplementedException();
            }

            public IEnumerator<KeyValuePair<KEY, VALUE>> GetEnumerator()
            {
                throw new NotImplementedException();
            }

            public bool Remove(KEY key)
            {
                throw new NotImplementedException();
            }

            public bool Remove(KeyValuePair<KEY, VALUE> item)
            {
                throw new NotImplementedException();
            }

            public bool TryGetValue(KEY key, out VALUE value)
            {
                VALUE valuer = default;
                VALUE input = default;
                var r = s.Read(ref key, ref valuer);
                //var r = s.Read(ref key, ref input, ref valuer, context, 0);
                if (r == Status.OK)
                {
                    value = (VALUE)valuer;
                    return true;
                }
                else
                {
                    throw new Exception("not okay");
                    value = default;
                    return false;
                }
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                throw new NotImplementedException();
            }

            internal void Flush()
            {

                store.Log.FlushAndEvict(true);
            }
        }

        //internal class MyFunctions<KEY, VALUE> : FunctionsBase<KEY, VALUE, Empty, VALUE, Empty>
        //{
        //}
    }



    public class MyValueSerializer<T> : BinaryObjectSerializer<T>, IFasterEqualityComparer<T>
    {
        public override void Serialize(ref T value)
        {
            BinaryFormatter bf = new BinaryFormatter();
            bf.Serialize(writer.BaseStream, value);
        }

        public override void Deserialize(out T value)
        {
            BinaryFormatter bf = new BinaryFormatter();
            value = (T)bf.Deserialize(reader.BaseStream);
        }

        public long GetHashCode64(ref T k)
        {
            // returns int hashcode, not as good as faster's expected long hashcode
            return k.GetHashCode();
        }

        public bool Equals(ref T k1, ref T k2)
        {
            return k1.Equals(k2);
        }
    }
}
