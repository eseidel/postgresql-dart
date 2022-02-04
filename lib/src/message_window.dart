import 'dart:collection';
import 'dart:typed_data';

import 'package:buffer/buffer.dart';

import 'connection_config.dart';
import 'server_messages.dart';

const int _headerByteSize = 5;
final _emptyData = Uint8List(0);

typedef _ServerMessageFn = ServerMessage Function(
    Uint8List data, ConnectionConfig config);

Map<int, _ServerMessageFn> _messageTypeMap = {
  49: (d, c) => ParseCompleteMessage(),
  50: (d, c) => BindCompleteMessage(),
  65: (d, c) => NotificationResponseMessage(d, c),
  67: (d, c) => CommandCompleteMessage(d, c),
  68: (d, c) => DataRowMessage(d),
  69: (d, c) => ErrorResponseMessage(d),
  75: (d, c) => BackendKeyMessage(d),
  82: (d, c) => AuthenticationMessage(d),
  83: (d, c) => ParameterStatusMessage(d, c),
  84: (d, c) => RowDescriptionMessage(d),
  90: (d, c) => ReadyForQueryMessage(d, c),
  110: (d, c) => NoDataMessage(),
  116: (d, c) => ParameterDescriptionMessage(d),
};

class MessageFramer {
  final ConnectionConfig _config;
  final _reader = ByteDataReader();
  final messageQueue = Queue<ServerMessage>();

  int? _type;
  int _expectedLength = 0;

  MessageFramer(this._config);

  bool get _hasReadHeader => _type != null;
  bool get _canReadHeader => _reader.remainingLength >= _headerByteSize;

  bool get _isComplete =>
      _expectedLength == 0 || _expectedLength <= _reader.remainingLength;

  void addBytes(Uint8List bytes) {
    _reader.add(bytes);

    var evaluateNextMessage = true;
    while (evaluateNextMessage) {
      evaluateNextMessage = false;

      if (!_hasReadHeader && _canReadHeader) {
        _type = _reader.readUint8();
        _expectedLength = _reader.readUint32() - 4;
      }

      if (_hasReadHeader && _isComplete) {
        final data =
            _expectedLength == 0 ? _emptyData : _reader.read(_expectedLength);
        final msgMaker = _messageTypeMap[_type];
        final msg = msgMaker == null
            ? UnknownMessage(_type, data)
            : msgMaker(data, _config);
        messageQueue.add(msg);
        _type = null;
        _expectedLength = 0;
        evaluateNextMessage = true;
      }
    }
  }

  bool get hasMessage => messageQueue.isNotEmpty;

  ServerMessage popMessage() {
    return messageQueue.removeFirst();
  }
}
