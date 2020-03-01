﻿/*
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
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Threading.Tasks;
    using Abstractions;
    using Events;

    public sealed class ProcessManager : IRegisterEvent, IAsyncDisposable
    {
        readonly IConnectionPool                      _connectionPool;
        readonly ConcurrentDictionary<Guid, IProcess> _processes;

        public ProcessManager(IConnectionPool connectionPool)
        {
            _processes      = new ConcurrentDictionary<Guid, IProcess>();
            _connectionPool = connectionPool;
        }

        public async ValueTask DisposeAsync()
        {
            var processes = _processes.Values.ToArray();

            for (var i = 0; i < processes.Length; ++i)
                await processes[i].DisposeAsync();

            await _connectionPool.DisposeAsync();
        }

        public void Register(IEvent e)
        {
            switch (e)
            {
                case ConsumerCreated _:
                    DotPulsarEventSource.Log.ConsumerCreated();
                    break;
                case ConsumerDisposed consumerDisposed:
                    Remove(consumerDisposed.CorrelationId);
                    DotPulsarEventSource.Log.ConsumerDisposed();
                    break;
                case ProducerCreated _:
                    DotPulsarEventSource.Log.ProducerCreated();
                    break;
                case ProducerDisposed producerDisposed:
                    Remove(producerDisposed.CorrelationId);
                    DotPulsarEventSource.Log.ProducerDisposed();
                    break;
                case ReaderCreated _:
                    DotPulsarEventSource.Log.ReaderCreated();
                    break;
                case ReaderDisposed readerDisposed:
                    Remove(readerDisposed.CorrelationId);
                    DotPulsarEventSource.Log.ReaderDisposed();
                    break;
                default:
                    if (_processes.TryGetValue(e.CorrelationId, out var process))
                        process.Handle(e);

                    break;
            }
        }

        public void Add(IProcess process) => _processes[process.CorrelationId] = process;

        async void Remove(Guid correlationId)
        {
            if (_processes.TryRemove(correlationId, out var process))
                await process.DisposeAsync();
        }
    }
}