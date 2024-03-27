The loader will generally be installed via NPM. There are various
things that can go wrong with NPM packaging - so we like to find
that out before we actually publish.

This project will execute PFDL both as a library and a CLI tool
to confirm basic packaging works.

```
pnpm i
pnpm run test-lib
pnpm run test-cli
```
