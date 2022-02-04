import 'package:crypto/crypto.dart';
import 'package:sasl_scram/sasl_scram.dart';

import '../../postgres.dart';
import '../connection_config.dart';
import '../server_messages.dart';
import 'clear_text_authenticator.dart';
import 'md5_authenticator.dart';
import 'sasl_authenticator.dart';

enum AuthenticationScheme { MD5, SCRAM_SHA_256, CLEAR }

abstract class PostgresAuthenticator {
  static String? name;
  final PostgreSQLConnection connection;
  final ConnectionConfig config;

  PostgresAuthenticator(this.connection, this.config);

  void onMessage(AuthenticationMessage message);
}

PostgresAuthenticator createAuthenticator(PostgreSQLConnection connection,
    ConnectionConfig config, AuthenticationScheme authenticationScheme) {
  switch (authenticationScheme) {
    case AuthenticationScheme.MD5:
      return MD5Authenticator(connection, config);
    case AuthenticationScheme.SCRAM_SHA_256:
      final credentials = UsernamePasswordCredential(
          username: connection.username, password: connection.password);
      return PostgresSaslAuthenticator(connection, config,
          ScramAuthenticator('SCRAM-SHA-256', sha256, credentials));
    case AuthenticationScheme.CLEAR:
      return ClearAuthenticator(connection, config);
    default:
      throw PostgreSQLException("Authenticator wasn't specified");
  }
}
