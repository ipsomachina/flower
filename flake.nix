{
  description = "Flower: Rust development, Wasm guests and release tooling";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    xmit = {
      url = "github:xmit-dev/xmit";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      xmit,
      ...
    }:
    let
      systems = [
        "aarch64-linux"
        "x86_64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          f (
            import nixpkgs {
              inherit system;
              overlays = [ rust-overlay.overlays.default ];
              config.allowDeprecatedx86_64Darwin = true;
            }
          )
        );
    in
    {
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          name = "flower-dev";

          packages = [
            # Keep aligned with the CI and Docker release toolchains.
            (pkgs.rust-bin.stable."1.99.1".minimal.override {
              extensions = [
                "clippy"
                "rustfmt"
                "rust-src"
                "rust-analyzer"
              ];
              targets = [ "wasm32-unknown-unknown" ];
            })
            pkgs.clang
            pkgs.cmake
            pkgs.pkg-config
            (import ./nix/bun.nix { inherit pkgs; })
            pkgs.git
            pkgs.actionlint
            xmit.packages.${pkgs.stdenv.hostPlatform.system}.default
          ];

          buildInputs = [ pkgs.openssl ];
          LIBCLANG_PATH = pkgs.lib.makeLibraryPath [ pkgs.llvmPackages.libclang.lib ];
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixfmt);
    };
}
