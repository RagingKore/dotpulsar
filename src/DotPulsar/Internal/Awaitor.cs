/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DotPulsar.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public sealed class Awaitor<T, TResult> : IDisposable
    {
        readonly Dictionary<T, TaskCompletionSource<TResult>> _items;

        public Awaitor() => _items = new Dictionary<T, TaskCompletionSource<TResult>>();

        public void Dispose()
        {
            foreach (var item in _items.Values) item.SetCanceled();

            _items.Clear();
        }

        public Task<TResult> CreateTask(T item)
        {
            var tcs = new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            _items.Add(item, tcs);
            return tcs.Task;
        }

        public void SetResult(T item, TResult result)
        {
#if NETSTANDARD2_0
            var tcs = _items[item];
            _items.Remove(item);
#else
            _items.Remove(item, out var tcs);
#endif
            tcs.SetResult(result);
        }
    }
}