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
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Abstractions;
    using DotPulsar.Abstractions;
    using Events;

    public sealed class Reader : IReader
    {
        readonly Guid                       _correlationId;
        readonly IRegisterEvent             _eventRegister;
        readonly IExecute                   _executor;
        readonly IStateChanged<ReaderState> _state;

        IReaderChannel _channel;
        int            _isDisposed;

        public Reader(
            Guid correlationId,
            IRegisterEvent eventRegister,
            IReaderChannel initialChannel,
            IExecute executor,
            IStateChanged<ReaderState> state)
        {
            _correlationId = correlationId;
            _eventRegister = eventRegister;
            _channel       = initialChannel;
            _executor      = executor;
            _state         = state;
            _isDisposed    = 0;

            _eventRegister.Register(new ReaderCreated(_correlationId, this));
        }

        public async ValueTask<ReaderState> StateChangedTo(ReaderState state, CancellationToken cancellationToken)
            => await _state.StateChangedTo(state, cancellationToken);

        public async ValueTask<ReaderState> StateChangedFrom(ReaderState state, CancellationToken cancellationToken)
            => await _state.StateChangedFrom(state, cancellationToken);

        public bool IsFinalState() => _state.IsFinalState();

        public bool IsFinalState(ReaderState state) => _state.IsFinalState(state);

        public async IAsyncEnumerable<Message> Messages([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            while (!cancellationToken.IsCancellationRequested)
                yield return await _executor.Execute(() => _channel.Receive(cancellationToken), cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
                return;

            _eventRegister.Register(new ReaderDisposed(_correlationId, this));
            await _channel.DisposeAsync();
        }

        internal void SetChannel(IReaderChannel channel)
        {
            ThrowIfDisposed();
            _channel = channel;
        }

        void ThrowIfDisposed()
        {
            if (_isDisposed != 0)
                throw new ObjectDisposedException(nameof(Reader));
        }
    }
}