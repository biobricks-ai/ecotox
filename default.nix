{ sources ? import ./nix/sources.nix, pkgs ? import sources.nixpkgs { } }:
with pkgs;
let jdk = pkgs.openjdk17;
in mkShell {
  buildInputs = [ (clojure.override { jdk = jdk; }) csv2parquet jdk rlwrap ];
}
