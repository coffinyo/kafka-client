import Config

config :logger,
  handle_otp_reports: true,
  level: :warning

case System.get_env("KAFKA_CLIENT_BACKEND") do
  nil -> :ok
  "java" -> config :kafka_client, backend: :java
  "rust" -> config :kafka_client, backend: :rust
end
