defmodule KafkaClient.MixProject do
  use Mix.Project

  def project do
    [
      app: :kafka_client,
      version: "0.1.0",
      elixir: "~> 1.13",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:crypto, :logger]
    ]
  end

  defp elixirc_paths(env) when env in [:test, :prod_test], do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:credo, "~> 1.6", only: [:dev, :test]},
      {:dialyxir, "~> 1.2", only: [:dev, :test], runtime: false},
      {:parent, "~> 0.12"},
      {:statistex, "~> 1.0", only: [:dev, :test]},
      {:telemetry, "~> 1.0"}
    ]
  end

  defp aliases do
    [
      compile: [&compile_port/1, "compile"]
    ]
  end

  # The port backend is selected with the KAFKA_CLIENT_BACKEND OS env, which should be set to the
  # same value when compiling and running. Only the selected port program is built.
  defp compile_port(_args) do
    case System.get_env("KAFKA_CLIENT_BACKEND", "java") do
      "java" -> compile_java_port()
      "rust" -> compile_rust_port()
    end
  end

  defp compile_java_port do
    src_files = ["port/pom.xml" | Path.wildcard("port/src/**/*.java")]

    if recompile_port?("port/target/src_hash", src_files) do
      Mix.shell().info("Recompiling java port")
      run_port_build("mvn", ~w/clean compile assembly:single/, "port")
      File.write("port/target/src_hash", src_hash(src_files))
      File.mkdir_p!("priv")

      File.cp(
        "port/target/kafka-client-1.0-SNAPSHOT-jar-with-dependencies.jar",
        "priv/kafka-client-1.0.jar"
      )
    end
  end

  defp compile_rust_port do
    root = "native/kafka_client_port"

    src_files = [
      "#{root}/Cargo.toml",
      "#{root}/Cargo.lock" | Path.wildcard("#{root}/src/**/*.rs")
    ]

    if recompile_port?("#{root}/target/src_hash", src_files) do
      Mix.shell().info("Recompiling rust port")
      run_port_build("cargo", ~w/build --release --locked/, root)
      File.write("#{root}/target/src_hash", src_hash(src_files))
      File.mkdir_p!("priv")
      File.cp("#{root}/target/release/kafka_client_port", "priv/kafka_client_port")
    end
  end

  defp run_port_build(command, args, dir) do
    case System.cmd(command, args, cd: dir, stderr_to_stdout: true) do
      {_output, 0} ->
        :ok

      {output, status} ->
        Mix.raise("#{output}\n\nPort project build exited with the status #{status}")
    end
  end

  defp recompile_port?(hash_file, src_files) do
    case File.read(hash_file) do
      {:ok, src_hash} -> src_hash != src_hash(src_files)
      {:error, _posix} -> true
    end
  end

  defp src_hash(src_files),
    do: :crypto.hash(:sha256, Enum.map(src_files, &File.read!/1))
end
