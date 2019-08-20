using System;
using System.IO;

namespace Rebus.Oracle.DataBus
{
    /// <summary>Wraps a stream and additional resources, disposing them when the stream is disposed</summary>
    class StreamWrapper : Stream
    {
        readonly Stream _innerStream;
        readonly IDisposable[] _disposables;

        public StreamWrapper(Stream innerStream, params IDisposable[] disposables)
        {
            if (innerStream == null) throw new ArgumentNullException(nameof(innerStream));
            _innerStream = innerStream;
            _disposables = disposables;
        }

        public override void Flush() => _innerStream.Flush();

        public override long Seek(long offset, SeekOrigin origin) => _innerStream.Seek(offset, origin);

        public override void SetLength(long value) => _innerStream.SetLength(value);

        public override int Read(byte[] buffer, int offset, int count) => _innerStream.Read(buffer, offset, count);

        public override void Write(byte[] buffer, int offset, int count) => _innerStream.Write(buffer, offset, count);

        public override bool CanRead => _innerStream.CanRead;
        public override bool CanSeek => _innerStream.CanSeek;
        public override bool CanWrite => _innerStream.CanWrite;
        public override long Length => _innerStream.Length;

        public override long Position
        {
            get => _innerStream.Position;
            set => _innerStream.Position = value;
        }

        protected override void Dispose(bool disposing)
        {
            _innerStream.Dispose();

            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }            
        }
    }
}