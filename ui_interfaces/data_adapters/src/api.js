export async function callConfig(path, { file, text, topicFamily } = {}) {
  const form = new FormData();
  if (file) form.append('file', file);
  else if (text) form.append('text', text);
  if (topicFamily) form.append('topic_family', topicFamily);
  const res = await fetch(path, { method: 'POST', body: form });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}