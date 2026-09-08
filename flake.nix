{
  description = "python-binary-memcached development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        python = pkgs.python312;

        # The default package has no TLS, so test/test_tls.py skips.
        memcached = pkgs.memcached.overrideAttrs (old: {
          buildInputs = (old.buildInputs or [ ]) ++ [ pkgs.openssl ];
          configureFlags = (old.configureFlags or [ ]) ++ [ "--enable-tls" ];
        });

        # Runtime and test dependencies from the pyproject.toml project table
        # and from the test dependency group.
        pythonEnv = python.withPackages (ps: with ps; [
          # runtime
          uhashring
          # test and lint
          pytest
          pytest-cov
          trustme
          # packaging and tooling
          pip
          setuptools
          build
          twine
          tox
          # docs
          sphinx
        ]);
      in
      {
        devShells.default = pkgs.mkShell {
          name = "python-binary-memcached";

          packages = [
            pythonEnv
            memcached
            pkgs.ruff
            pkgs.pre-commit
            pkgs.commitizen
          ];

          env = {
            PYTHONPATH = ".";
            MEMCACHED_HOST = "localhost";
          };

          shellHook = ''
            echo "python-binary-memcached dev shell"
            echo "  python:    $(python --version)"
            echo "  memcached: $(memcached --version)"
            echo "  run tests: pytest -s"
          '';
        };

        packages.default = python.pkgs.buildPythonPackage {
          pname = "python-binary-memcached";
          version = "0.32.0";
          format = "setuptools";
          src = ./.;
          propagatedBuildInputs = with python.pkgs; [ uhashring ];
          doCheck = false;
        };

        formatter = pkgs.nixpkgs-fmt;
      });
}
