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
    using System.Threading;
    using System.Threading.Tasks;

    public sealed class AsyncLock : IAsyncDisposable
    {
        readonly Task<IDisposable>                                   _completedTask;
        readonly LinkedList<CancelableCompletionSource<IDisposable>> _pending;
        readonly Releaser                                            _releaser;
        readonly SemaphoreSlim                                       _semaphoreSlim;

        bool _isDisposed;

        public AsyncLock()
        {
            _pending       = new LinkedList<CancelableCompletionSource<IDisposable>>();
            _semaphoreSlim = new SemaphoreSlim(1, 1);
            _releaser      = new Releaser(Release);
            _completedTask = Task.FromResult((IDisposable) _releaser);
            _isDisposed    = false;
        }

        public async ValueTask DisposeAsync()
        {
            lock (_pending)
            {
                if (_isDisposed)
                    return;

                _isDisposed = true;

                foreach (var pending in _pending)
                {
                    pending.SetException(new ObjectDisposedException(nameof(AsyncLock)));
                    pending.Dispose();
                }

                _pending.Clear();
            }

            await _semaphoreSlim.WaitAsync(); //Wait for possible lock-holder to finish
            _semaphoreSlim.Release();
            _semaphoreSlim.Dispose();
        }

        public Task<IDisposable> Lock(CancellationToken cancellationToken = default)
        {
            LinkedListNode<CancelableCompletionSource<IDisposable>>? node;

            lock (_pending)
            {
                if (_isDisposed)
                    throw new ObjectDisposedException(nameof(AsyncLock));

                if (_semaphoreSlim.CurrentCount == 1) //Lock is free
                {
                    _semaphoreSlim.Wait(cancellationToken); //Will never block
                    return _completedTask;
                }

                //Lock was not free
                node = _pending.AddLast(new CancelableCompletionSource<IDisposable>());
            }

            cancellationToken.Register(() => Cancel(node));

            return node.Value.Task;
        }

        void Cancel(LinkedListNode<CancelableCompletionSource<IDisposable>> node)
        {
            lock (_pending)
            {
                try
                {
                    _pending.Remove(node);
                    node.Value.Dispose();
                }
                catch
                {
                    // TODO RK: Ignored and yet we should probably log it.
                }
            }
        }

        void Release()
        {
            lock (_pending)
            {
                if (_pending.Count > 0)
                {
                    var node = _pending.First;
                    node.Value.SetResult(_releaser);
                    node.Value.Dispose();
                    _pending.RemoveFirst();
                    return;
                }

                if (_semaphoreSlim.CurrentCount == 0)
                    _semaphoreSlim.Release();
            }
        }

        class Releaser : IDisposable
        {
            readonly Action _release;

            public Releaser(Action release) => _release = release;

            public void Dispose() => _release();
        }
    }
}