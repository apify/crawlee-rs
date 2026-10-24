// Writes crates/crawlee-http/src/mime_types.rs from the mime-types that @crawlee/http resolves:
//   node conformance/oracle/generate-mime-types.cjs ../crawlee/packages/http-crawler > crates/crawlee-http/src/mime_types.rs
// (needs a crawlee checkout with `pnpm install` done).
const mime = require(require.resolve('mime-types', { paths: [process.argv[2]] }));
const version = (name, from) => require(require.resolve(`${name}/package.json`, { paths: [from] })).version;
const mtDir = require.resolve('mime-types', { paths: [process.argv[2]] });
const rows = Object.keys(mime.types).sort().map((ext) => {
    const [type, param] = mime.contentType(`.${ext}`).split('; charset=');
    return `    (${JSON.stringify(ext)}, ${JSON.stringify(type)}, ${param ? `Some(${JSON.stringify(param)})` : 'None'}),`;
});
console.log(`//! The extension table of \`mime-types\` ${version('mime-types', process.argv[2])} (mime-db ${version('mime-db', mtDir)}), which
//! Crawlee for JS uses to guess the content type of a response without a \`Content-Type\` header.
//!
//! Generated: \`mime.contentType('.' + extension)\` for every extension, split into the type and
//! the charset it adds. Regenerate with \`conformance/oracle/generate-mime-types.cjs\`.

/// \`(extension, type, charset)\`, sorted by extension.
pub(crate) static EXTENSIONS: &[(&str, &str, Option<&str>)] = &[
${rows.join('\n')}
];`);
