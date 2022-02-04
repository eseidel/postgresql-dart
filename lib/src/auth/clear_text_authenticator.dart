import 'dart:convert';

import 'package:buffer/buffer.dart';

import '../../postgres.dart';
import '../client_messages.dart';
import '../connection_config.dart';
import '../server_messages.dart';
import '../utf8_backed_string.dart';
import 'auth.dart';

class ClearAuthenticator extends PostgresAuthenticator {
  ClearAuthenticator(PostgreSQLConnection connection, ConnectionConfig config)
      : super(connection, config);

  @override
  void onMessage(AuthenticationMessage message) {
    final authMessage = ClearMessage(connection.password!, config.encoding);
    connection.socket!.add(authMessage.asBytes());
  }
}

class ClearMessage extends ClientMessage {
  UTF8BackedString? _authString;

  ClearMessage(String password, Encoding encoding) {
    _authString = UTF8BackedString(password, encoding);
  }

  @override
  void applyToBuffer(ByteDataWriter buffer) {
    buffer.writeUint8(ClientMessage.PasswordIdentifier);
    final length = 5 + _authString!.utf8Length;
    buffer.writeUint32(length);
    _authString!.applyToBuffer(buffer);
  }
}
