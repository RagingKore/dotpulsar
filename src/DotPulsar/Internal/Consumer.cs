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
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Abstractions;
    using DotPulsar.Abstractions;
    using Events;
    using PulsarApi;

    public sealed class Consumer : IConsumer
    {
        readonly CommandAck                   _cachedCommandAck;
        readonly Guid                         _correlationId;
        readonly IRegisterEvent               _eventRegister;
        readonly IExecute                     _executor;
        readonly IStateChanged<ConsumerState> _state;
        IConsumerChannel             _channel;
        int                          _isDisposed;

        public Consumer(
            Guid correlationId,
            IRegisterEvent eventRegister,
            IConsumerChannel initialChannel,
            IExecute executor,
            IStateChanged<ConsumerState> state)
        {
            _correlationId    = correlationId;
            _eventRegister    = eventRegister;
            _channel          = initialChannel;
            _executor         = executor;
            _state            = state;
            _cachedCommandAck = new CommandAck();
            _isDisposed       = 0;

            _eventRegister.Register(new ConsumerCreated(_correlationId, this));
        }

        public async ValueTask<ConsumerState> StateChangedTo(ConsumerState state, CancellationToken cancellationToken)
            => await _state.StateChangedTo(state, cancellationToken);

        public async ValueTask<ConsumerState> StateChangedFrom(ConsumerState state, CancellationToken cancellationToken)
            => await _state.StateChangedFrom(state, cancellationToken);

        public bool IsFinalState() => _state.IsFinalState();

        public bool IsFinalState(ConsumerState state) => _state.IsFinalState(state);

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
                return;

            _eventRegister.Register(new ConsumerDisposed(_correlationId, this));
            await _channel.DisposeAsync();
        }

        public async IAsyncEnumerable<Message> Messages([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            while (!cancellationToken.IsCancellationRequested)
                yield return await _executor.Execute(() => _channel.Receive(cancellationToken), cancellationToken);
        }

        public async ValueTask Acknowledge(Message message, CancellationToken cancellationToken)
            => await Acknowledge(message.MessageId.Data, CommandAck.AckType.Individual, cancellationToken);

        public async ValueTask Acknowledge(MessageId messageId, CancellationToken cancellationToken)
            => await Acknowledge(messageId.Data, CommandAck.AckType.Individual, cancellationToken);

        public async ValueTask AcknowledgeCumulative(Message message, CancellationToken cancellationToken)
            => await Acknowledge(message.MessageId.Data, CommandAck.AckType.Cumulative, cancellationToken);

        public async ValueTask AcknowledgeCumulative(MessageId messageId, CancellationToken cancellationToken)
            => await Acknowledge(messageId.Data, CommandAck.AckType.Cumulative, cancellationToken);

        public async ValueTask Unsubscribe(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            _ = await _executor.Execute(() => _channel.Send(new CommandUnsubscribe()), cancellationToken);
        }

        public async ValueTask Seek(MessageId messageId, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            _ = await _executor.Execute(() => _channel.Send(new CommandSeek { MessageId = messageId.Data }), cancellationToken);
        }

        public async ValueTask<MessageId> GetLastMessageId(CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            var response = await _executor.Execute(() => _channel.Send(new CommandGetLastMessageId()), cancellationToken);
            return new MessageId(response.LastMessageId);
        }

        async ValueTask Acknowledge(MessageIdData messageIdData, CommandAck.AckType ackType, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            await _executor.Execute(
                () =>
                {
                    _cachedCommandAck.Type = ackType;
                    _cachedCommandAck.MessageIds.Clear();
                    _cachedCommandAck.MessageIds.Add(messageIdData);
                    return _channel.Send(_cachedCommandAck);
                }, cancellationToken
            );
        }

        internal void SetChannel(IConsumerChannel channel)
        {
            ThrowIfDisposed();
            _channel = channel;
        }

        void ThrowIfDisposed()
        {
            if (_isDisposed != 0)
                throw new ObjectDisposedException(GetType().FullName);
        }
    }
}