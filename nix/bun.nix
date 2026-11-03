# Pin Bun for repository tools that require its XML API (Bun >= 1.4).
# Archive hashes are from the upstream release's SHASUMS256.txt.
{ pkgs }:
let
  version = "1.4.4";
  sources = {
    aarch64-linux = {
      archive = "bun-linux-aarch64";
      hash = "sha256-uqa7NvUdblhWHgK76oJQOKnfRU9nPW13bou/hvjHhQ4=";
    };
    x86_64-linux = {
      archive = "bun-linux-x64";
      hash = "sha256-v4zL+ysLgBWeD5ptFcVTF63YB3U028YTCj7bUvzLOVw=";
    };
    aarch64-darwin = {
      archive = "bun-darwin-aarch64";
      hash = "sha256-wkDX8My5DCY6hJ/NsJt+5iYw2SE0jtaq/ahrnVd7xRA=";
    };
    x86_64-darwin = {
      archive = "bun-darwin-x64-baseline";
      hash = "sha256-1KFVrfv9CRVes7oFyeOlRPOSl9emhhbTx3WfZ6tcR9w=";
    };
  };
  source = sources.${pkgs.stdenv.hostPlatform.system};
in
pkgs.bun.overrideAttrs (_: {
  inherit version;
  src = pkgs.fetchurl {
    url = "https://github.com/oven-sh/bun/releases/download/bun-v${version}/${source.archive}.zip";
    inherit (source) hash;
  };
})
