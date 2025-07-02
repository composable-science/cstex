{
  description = "CSTeX Compilation Flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    texMini.url = "github:composable-science/texMini";
  };

  outputs = { self, nixpkgs, flake-utils, texMini }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        pythonEnv = pkgs.python313.withPackages (ps: with ps; [
          pandas
          numpy
          matplotlib
          tomli
          toml
          click
          cryptography
          requests
          rich
          jinja2
          pyyaml
        ]);

        # Package for the compilation scripts
        csf-scripts = pkgs.stdenv.mkDerivation {
          name = "csf-scripts";
          src = ./scripts;
          # We no longer need to build a custom TeX env here.
          # The texMini flake will provide it at runtime.
          installPhase = ''
            mkdir -p $out/bin
            cp $src/* $out/bin/
            chmod +x $out/bin/*
          '';
        };

        # The main cstex-compile package, now using texMini
        cstex-compile = pkgs.writeShellApplication {
          name = "cstex-compile";
          runtimeInputs = [
            pythonEnv
            csf-scripts # Our helper scripts
            texMini.packages.${system}.default # The texMini environment
          ];
          text = ''
            #!${pkgs.bash}/bin/bash
            # The cstex-compile.sh script will now have latexmk in its PATH
            # and can call it directly.
            export TEXINPUTS=".:${self}/../core/templates:"
            exec cstex-compile.sh "$@"
          '';
        };

      in
      {
        packages = {
          default = cstex-compile;
          cstex-compile = cstex-compile;
          scripts = csf-scripts;
        };

        apps = {
          default = flake-utils.lib.mkApp { drv = cstex-compile; };
          cstex-compile = flake-utils.lib.mkApp { drv = cstex-compile; };
        };
      });
}