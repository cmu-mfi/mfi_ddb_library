import { useCallback, useMemo, useState } from "react";
import { ChevronDown, ChevronRight } from "lucide-react";

/**
 * Collapsible sections. Every field belongs to the section heading above it
 * (until the next section). Collapsing a section hides its fields; sections
 * holding a field with an error can be re-opened with `reveal(errorPaths)`.
 */
export function useSections(fields) {
  const [collapsed, setCollapsed] = useState(() => new Set());

  const { sectionOf, counts } = useMemo(() => {
    const of = {};
    const n = {};
    let current = null;
    for (const f of fields) {
      if (f.type === "section") current = f.name;
      else if (current) {
        of[f.name] = current;
        n[current] = (n[current] || 0) + 1;
      }
    }
    return { sectionOf: of, counts: n };
  }, [fields]);

  const toggle = useCallback(
    (name) =>
      setCollapsed((prev) => {
        const next = new Set(prev);
        if (next.has(name)) next.delete(name);
        else next.add(name);
        return next;
      }),
    [],
  );

  /** Open every section that holds one of these field paths ("name" or "list.0.sub"). */
  const reveal = useCallback(
    (paths) =>
      setCollapsed((prev) => {
        const next = new Set(prev);
        for (const path of paths) next.delete(sectionOf[String(path).split(".")[0]]);
        return next.size === prev.size ? prev : next;
      }),
    [sectionOf],
  );

  const reset = useCallback(() => setCollapsed(new Set()), []);

  return {
    isHidden: (field) => field.type !== "section" && collapsed.has(sectionOf[field.name]),
    isCollapsed: (name) => collapsed.has(name),
    count: (name) => counts[name] || 0,
    toggle,
    reveal,
    reset,
  };
}

/** Clickable section heading: arrow, title, description, and a count when collapsed. */
export function SectionHeader({ field, sections }) {
  const isCollapsed = sections.isCollapsed(field.name);
  const count = sections.count(field.name);
  return (
    <div className="mt-3 border-t border-gray-200 pt-4 first:mt-0 first:border-t-0 first:pt-0 sm:col-span-2">
      <button
        type="button"
        aria-expanded={!isCollapsed}
        onClick={() => sections.toggle(field.name)}
        className="group flex w-full items-center gap-2 text-left"
        title={isCollapsed ? "Show this section" : "Hide this section"}
      >
        {isCollapsed ? (
          <ChevronRight size={18} className="shrink-0 text-gray-400 group-hover:text-gray-700" />
        ) : (
          <ChevronDown size={18} className="shrink-0 text-gray-400 group-hover:text-gray-700" />
        )}
        <h3 className="text-base font-semibold text-gray-800">{field.label}</h3>
        {isCollapsed && count > 0 && (
          <span className="text-xs text-gray-500">
            {count} field{count === 1 ? "" : "s"} hidden
          </span>
        )}
      </button>
      {field.help && !isCollapsed && <p className="mt-0.5 pl-[26px] text-sm text-gray-500">{field.help}</p>}
    </div>
  );
}
