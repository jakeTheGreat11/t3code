import { resolveSidebarDropTarget, type SidebarListItem } from "./Sidebar.logic";

type HierarchyThread = {
  readonly id: string;
  readonly environmentId: string;
  readonly coordinatorThreadId?: string;
};

export const sidebarHierarchyKey = (thread: HierarchyThread) =>
  `${thread.environmentId}:${thread.id}`;

/** Input is the visible roster in its existing order, before shelf paging.
 * Missing/filtered coordinators leave workers at the top level. */
export function buildSidebarHierarchy<T extends HierarchyThread>(threads: readonly T[]) {
  const byKey = new Map(threads.map((thread) => [sidebarHierarchyKey(thread), thread]));
  const parentByKey = new Map<string, string>();
  for (const thread of threads) {
    const key = sidebarHierarchyKey(thread);
    const parent = `${thread.environmentId}:${thread.coordinatorThreadId}`;
    if (thread.coordinatorThreadId !== undefined && key !== parent && byKey.has(parent)) {
      parentByKey.set(key, parent);
    }
  }
  // Malformed cycles must not make threads disappear or hang the renderer.
  const visited = new Set<string>();
  for (const key of byKey.keys()) {
    const path = new Set<string>();
    let next: string | undefined = key;
    while (next !== undefined && !visited.has(next) && !path.has(next)) {
      path.add(next);
      next = parentByKey.get(next);
    }
    if (next !== undefined && path.has(next)) {
      const cycleStart = next;
      let cycleKey = cycleStart;
      do {
        const parent = parentByKey.get(cycleKey)!;
        parentByKey.delete(cycleKey);
        cycleKey = parent;
      } while (cycleKey !== cycleStart);
    }
    for (const member of path) visited.add(member);
  }
  const childrenByKey = new Map<string, T[]>();
  const roots: T[] = [];
  for (const thread of threads) {
    const parent = parentByKey.get(sidebarHierarchyKey(thread));
    if (parent === undefined) roots.push(thread);
    else {
      const children = childrenByKey.get(parent) ?? [];
      children.push(thread);
      childrenByKey.set(parent, children);
    }
  }
  const rootByKey = new Map<string, string>();
  const depthByKey = new Map<string, number>();
  const pending = roots.map((thread) => ({ thread, root: sidebarHierarchyKey(thread), depth: 0 }));
  for (const { thread, root, depth } of pending) {
    const key = sidebarHierarchyKey(thread);
    rootByKey.set(key, root);
    depthByKey.set(key, depth);
    for (const child of childrenByKey.get(key) ?? []) {
      pending.push({ thread: child, root, depth: depth + 1 });
    }
  }
  return { roots, parentByKey, childrenByKey, rootByKey, depthByKey };
}

export function expandSidebarHierarchy<T extends HierarchyThread>(
  roots: readonly T[],
  childrenByKey: ReadonlyMap<string, readonly T[]>,
  collapsed: ReadonlySet<string>,
) {
  const result: T[] = [];
  const pending = [...roots].reverse();
  while (pending.length > 0) {
    const thread = pending.pop()!;
    result.push(thread);
    const key = sidebarHierarchyKey(thread);
    if (collapsed.has(key)) continue;
    const children = childrenByKey.get(key) ?? [];
    for (let index = children.length - 1; index >= 0; index--) pending.push(children[index]!);
  }
  return result;
}

/** Reveal an explicitly selected thread without changing other collapse preferences. */
export function revealSidebarThreadAncestors(
  threadKey: string,
  parentByKey: ReadonlyMap<string, string>,
  collapsed: readonly string[],
) {
  const ancestors = new Set<string>();
  let parent = parentByKey.get(threadKey);
  while (parent !== undefined && !ancestors.has(parent)) {
    ancestors.add(parent);
    parent = parentByKey.get(parent);
  }
  return collapsed.some((key) => ancestors.has(key))
    ? collapsed.filter((key) => !ancestors.has(key))
    : collapsed;
}

/** Drop positions and order-key writes belong to roots, never to their workers. */
export function resolveSidebarHierarchyDropTarget(
  items: readonly SidebarListItem[],
  activeKey: string,
  overId: string,
  rootByKey?: ReadonlyMap<string, string>,
) {
  if (rootByKey === undefined) return resolveSidebarDropTarget(items, activeKey, overId);
  if ((rootByKey.get(activeKey) ?? activeKey) !== activeKey) return null;
  return resolveSidebarDropTarget(
    items.filter(
      (item) => item.kind !== "thread" || (rootByKey.get(item.key) ?? item.key) === item.key,
    ),
    activeKey,
    rootByKey.get(overId) ?? overId,
  );
}
