if Mix.env() == :prod_test and System.get_env("KAFKA_CLIENT_TEST_BROKER") == nil do
  ExUnit.start(exclude: [:require_kafka])
else
  KafkaClient.Test.Helper.initialize!()
  ExUnit.start()
end
