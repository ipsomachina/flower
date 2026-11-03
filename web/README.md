# Flower website

The landing page and documentation are built with
[Build Awesome](https://github.com/11ty/buildawesome). The project pins
`@awesome.me/buildawesome` to `4.0.0-alpha.10`: Build Awesome is currently a
prerelease, and the npm `latest` package is only a placeholder.

Use the repository's Bun 1.4.4 development shell. From the repository root:

```sh
bun install --cwd web --frozen-lockfile
bun run --cwd web dev
```

The development server prints its local URL and rebuilds when pages, layouts,
assets or included Rust examples change.

```sh
bun run --cwd web build   # generate web/_site/
bun run --cwd web check   # build, test helpers and check protocol exports
```

Deploy **`web/_site/`** as the static document root. The build preserves the
existing `.html` URLs, `/docs/` navigation, search and downloadable schemas.
The generated site has no server-side runtime and can also be opened directly
from disk. Dependencies and generated output are ignored by Git.

To deploy to `flower.ipsomachina.com` through
[xmit](https://github.com/xmit-dev/xmit), configure your key once with
`nix develop --command xmit set-key`, or supply it in `XMIT_KEY`. Then run:

```sh
nix develop --command bash scripts/deploy-web.sh
```

The script installs the locked dependencies, builds and checks the site,
and uploads only `web/_site/`. It works from any working directory when
invoked by its path.

`index.html` is the landing page. Its styles, fonts and images are copied to
the output along with the root repository's `LICENSE`. Documentation pages
contain only their page content; their shared layout is
`_includes/docs.11ty.js`, and navigation lives in `_data/navigation.js`.
See [the documentation editing guide](docs/README.md) for code examples.

Every build checks links, anchors, scripts and stylesheets across the entire
output. The output contains only public pages and assets; source tooling and
the Rust example workspace are not deployed.
