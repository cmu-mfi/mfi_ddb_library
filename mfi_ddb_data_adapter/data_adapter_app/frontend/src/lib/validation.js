export function formatValidationErrors(label, errors) {
  return `${label}:\n${errors.map(issue => {
    const path = (issue.loc || []).reduce((text, key) => typeof key === "number"
      ? `${text}[${key}]` : `${text}${text ? "." : ""}${key}`, "") || "configuration";
    const position = issue.line
      ? ` (line ${issue.line}${issue.column ? `, column ${issue.column}` : ""})` : "";
    return `  ${path}${position}: ${issue.message}`;
  }).join("\n")}`;
}
