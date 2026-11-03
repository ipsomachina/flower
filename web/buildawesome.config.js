import { validateSite } from "./tools/check-site.mjs";

export const directories = { input: ".", output: "_site", includes: "_includes", data: "_data" };

export default function(config) {
  config.ignores.add("docs/examples/**");
  config.ignores.add("docs/tools/**");
  config.ignores.add("tools/**");
  config.ignores.add("_lib/**");

  for (const asset of ["styles.css", "favicon.svg", "social.svg", "social.png", "fonts",
    "docs/assets/docs.css", "docs/assets/docs.js", "docs/assets/protocol"]) {
    config.addPassthroughCopy(asset);
  }
  config.addPassthroughCopy({ "../LICENSE": "LICENSE" });

  for (const example of ["quickstart", "snippets", "client"]) {
    config.addWatchTarget(`docs/examples/${example}/src/**/*.rs`);
  }
  config.addWatchTarget("../LICENSE");
  config.on("buildawesome.after", ({ directories }) => validateSite(directories.output));

  return {
    templateFormats: ["html", "11ty.js"],
    htmlTemplateEngine: false,
  };
}
