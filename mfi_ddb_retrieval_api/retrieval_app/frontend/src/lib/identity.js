// Who is asking. RWS filters trials by this user's access.
//
// Today the person types their user ID and domain in "Requesting as". It is
// not remembered: every visit starts empty, so nobody searches as the previous
// user of a shared computer. With single sign-on, the login proxy will know
// the user: replace loadIdentity() with the logged-in user (e.g. a /whoami
// endpoint the proxy answers) and hide the bar. Nothing else in the app reads
// identity directly - everything goes through here.

export function loadIdentity() {
  return { user_id: "", user_domain: "" };
}

/** Can requests be made as this identity? status.state: "empty" | "set" */
export function identityBlocked(status) {
  return !status || status.state === "empty";
}
