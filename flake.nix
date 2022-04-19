{
  description = "automergeable";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            overlays = [ rust-overlay.overlay ];
            system = system;
          };
          rust = pkgs.rust-bin.stable.latest.default;
          cargoNix = pkgs.callPackage ./Cargo.nix { };
        in
        rec
        {
          packages = {
            automergeable = cargoNix.workspaceMembers.automergeable.build;
            automergeable-traits = cargoNix.workspaceMembers.automergeable-traits.build;
            automergeable-derive = cargoNix.workspaceMembers.automergeable-derive.build;
          };

          defaultPackage = packages.automergeable;

          devShell = pkgs.mkShell {
            buildInputs = with pkgs;[
              (rust.override {
                extensions = [ "rust-src" "rustfmt" ];
                targets = [ "wasm32-unknown-unknown" ];
              })
              cargo-edit
              cargo-watch
              cargo-udeps
              cargo-expand
              cargo-insta
              crate2nix
              rust-analyzer

              wasm-pack
              nodejs

              rnix-lsp
              nixpkgs-fmt
            ];
          };
        }
      );
}
