import { describe, expect, it } from "vite-plus/test";
import { ThreadId } from "@t3tools/contracts";
import { sortSettledThreads } from "@t3tools/client-runtime/state/thread-sort";
import {
  filterSidebarV2VisibleThreads,
  sortPinnedThreadsForSidebar,
  sortThreadsForSidebar,
} from "./Sidebar.logic";
import {
  buildSidebarHierarchy,
  expandSidebarHierarchy,
  revealSidebarThreadAncestors,
  sidebarHierarchyKey,
} from "./Sidebar.hierarchy";

const coordinator = { id: "coordinator", environmentId: "local" };
const worker = { id: "worker", environmentId: "local", coordinatorThreadId: coordinator.id };
const unrelated = { id: "unrelated", environmentId: "local" };

describe("sidebar hierarchy ordering", () => {
  it.each(["active", "pinned", "settled"] as const)(
    "keeps the coordinator first and unrelated %s threads ordered as worker activity changes",
    (section) => {
      const timestamps = [1, 2, 3, 4].map((day) => `2026-10-0${day}T00:00:00.000Z`);
      const sortable = (id: string, timestamp: string, orderKey: string) => ({
        id,
        environmentId: "local",
        createdAt: timestamp,
        updatedAt: timestamp,
        latestUserMessageAt: timestamp,
        latestRun: null,
        settledAt: null,
        activeOrderKey: orderKey,
        pinOrderKey: orderKey,
      });
      const before = sortable("before", timestamps[2]!, "A");
      const parent = sortable(coordinator.id, timestamps[1]!, "B");
      const after = sortable("after", timestamps[0]!, "C");
      const child = {
        ...sortable(worker.id, timestamps[3]!, "0"),
        coordinatorThreadId: coordinator.id,
      };
      for (const activityAt of [timestamps[0]!, "2026-10-30T00:00:00.000Z"]) {
        const roster = [
          after,
          parent,
          before,
          {
            ...child,
            updatedAt: activityAt,
            latestUserMessageAt: activityAt,
          },
        ];
        const ordered =
          section === "active"
            ? sortThreadsForSidebar(roster)
            : section === "pinned"
              ? sortPinnedThreadsForSidebar(roster)
              : sortSettledThreads(roster);
        const hierarchy = buildSidebarHierarchy(ordered);
        const displayed = expandSidebarHierarchy(
          hierarchy.roots,
          hierarchy.childrenByKey,
          new Set(),
        );
        expect(displayed.map((thread) => thread.id)).toEqual([
          "before",
          coordinator.id,
          worker.id,
          "after",
        ]);
        expect(hierarchy.roots.map((thread) => thread.id)).toEqual([
          "before",
          coordinator.id,
          "after",
        ]);
      }
    },
  );

  it("keeps sibling order while gathering workers that precede or follow their coordinator", () => {
    const firstWorker = { ...worker, id: "first-worker" };
    const secondWorker = { ...worker, id: "second-worker" };
    const roster = [firstWorker, unrelated, coordinator, secondWorker];
    const hierarchy = buildSidebarHierarchy(roster);
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set())).toEqual([
      unrelated,
      coordinator,
      firstWorker,
      secondWorker,
    ]);
    expect(roster).toEqual([firstWorker, unrelated, coordinator, secondWorker]);
  });

  it("preserves every row's order and identity in an unrelated roster", () => {
    const roster = [unrelated, coordinator, { ...unrelated, id: "another" }];
    const hierarchy = buildSidebarHierarchy(roster);
    const displayed = expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set());
    expect(displayed).toEqual(roster);
    displayed.forEach((thread, index) => expect(thread).toBe(roster[index]));
    expect(hierarchy.parentByKey.size).toBe(0);
    expect(hierarchy.childrenByKey.size).toBe(0);
  });

  it("renders nested groups before later siblings and tracks their depth and outer root", () => {
    const nestedWorker = { ...worker, id: "nested", coordinatorThreadId: worker.id };
    const sibling = { ...worker, id: "sibling" };
    const hierarchy = buildSidebarHierarchy([
      nestedWorker,
      worker,
      unrelated,
      sibling,
      coordinator,
    ]);
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set())).toEqual([
      unrelated,
      coordinator,
      worker,
      nestedWorker,
      sibling,
    ]);
    expect(hierarchy.depthByKey.get(sidebarHierarchyKey(coordinator))).toBe(0);
    expect(hierarchy.depthByKey.get(sidebarHierarchyKey(worker))).toBe(1);
    expect(hierarchy.depthByKey.get(sidebarHierarchyKey(nestedWorker))).toBe(2);
    expect(hierarchy.rootByKey.get(sidebarHierarchyKey(nestedWorker))).toBe(
      sidebarHierarchyKey(coordinator),
    );
    expect(hierarchy.parentByKey.get(sidebarHierarchyKey(nestedWorker))).toBe(
      sidebarHierarchyKey(worker),
    );
  });
});

describe("unavailable coordinators and environment boundaries", () => {
  const record = (
    thread: typeof coordinator & { coordinatorThreadId?: string },
    projectId: string,
  ) => ({
    ...thread,
    projectId,
    archivedAt: null as string | null,
    lineage: {
      rootThreadId: ThreadId.make(thread.id),
      parentThreadId: null,
      relationshipToParent: null,
    },
  });

  it.each(["missing", "archived", "filtered"] as const)(
    "keeps a worker visible at the top level when its coordinator is %s",
    (state) => {
      const parent = {
        ...record(coordinator, "coordinators"),
        archivedAt: state === "archived" ? "2026-10-01T00:00:00.000Z" : null,
      };
      const child = record(worker, "workers");
      const other = record(unrelated, "workers");
      const visible = filterSidebarV2VisibleThreads(
        state === "missing" ? [child, other] : [child, parent, other],
        state === "filtered" ? new Set(["local:workers"]) : null,
      );
      const hierarchy = buildSidebarHierarchy(visible);
      expect(
        expandSidebarHierarchy(
          hierarchy.roots,
          hierarchy.childrenByKey,
          new Set([sidebarHierarchyKey(coordinator)]),
        ),
      ).toEqual([child, other]);
      expect(hierarchy.parentByKey.has(sidebarHierarchyKey(child))).toBe(false);
      expect(hierarchy.depthByKey.get(sidebarHierarchyKey(child))).toBe(0);
    },
  );

  it("removes archived workers without leaving an empty coordinator group", () => {
    const parent = record(coordinator, "project");
    const child = { ...record(worker, "project"), archivedAt: "2026-10-01T00:00:00.000Z" };
    const hierarchy = buildSidebarHierarchy(filterSidebarV2VisibleThreads([parent, child], null));
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set())).toEqual([
      parent,
    ]);
    expect(hierarchy.childrenByKey.has(sidebarHierarchyKey(parent))).toBe(false);
  });

  it("restores grouping when an unavailable coordinator reappears", () => {
    const orphaned = buildSidebarHierarchy([worker, unrelated]);
    expect(expandSidebarHierarchy(orphaned.roots, orphaned.childrenByKey, new Set())).toEqual([
      worker,
      unrelated,
    ]);
    const restored = buildSidebarHierarchy([worker, coordinator, unrelated]);
    expect(expandSidebarHierarchy(restored.roots, restored.childrenByKey, new Set())).toEqual([
      coordinator,
      worker,
      unrelated,
    ]);
  });

  it("does not mistake fork or subagent lineage for an independent worker relationship", () => {
    const parent = record(coordinator, "project");
    const fork = {
      ...record({ id: "fork", environmentId: "local" }, "project"),
      lineage: {
        rootThreadId: ThreadId.make(coordinator.id),
        parentThreadId: ThreadId.make(coordinator.id),
        relationshipToParent: "fork" as const,
      },
    };
    const subagent = {
      ...fork,
      id: "subagent",
      lineage: { ...fork.lineage, relationshipToParent: "subagent" as const },
    };
    const hierarchy = buildSidebarHierarchy(
      filterSidebarV2VisibleThreads([fork, subagent, parent], null),
    );
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set())).toEqual([
      fork,
      parent,
    ]);
    expect(hierarchy.parentByKey.size).toBe(0);
  });

  it("groups duplicate thread IDs only with coordinators from the same environment", () => {
    const remoteParent = { ...coordinator, environmentId: "remote" };
    const remoteWorker = { ...worker, environmentId: "remote" };
    const hierarchy = buildSidebarHierarchy([remoteWorker, worker, coordinator, remoteParent]);
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set())).toEqual([
      coordinator,
      worker,
      remoteParent,
      remoteWorker,
    ]);
    expect(hierarchy.rootByKey.get(sidebarHierarchyKey(remoteWorker))).toBe(
      sidebarHierarchyKey(remoteParent),
    );
    expect(hierarchy.rootByKey.get(sidebarHierarchyKey(worker))).toBe(
      sidebarHierarchyKey(coordinator),
    );
    const collapsed = expandSidebarHierarchy(
      hierarchy.roots,
      hierarchy.childrenByKey,
      new Set([sidebarHierarchyKey(coordinator)]),
    );
    expect(collapsed).toEqual([coordinator, remoteParent, remoteWorker]);
  });

  it("leaves a worker ungrouped when the matching coordinator exists only in another environment", () => {
    const remoteParent = { ...coordinator, environmentId: "remote" };
    const hierarchy = buildSidebarHierarchy([worker, remoteParent]);
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set())).toEqual([
      worker,
      remoteParent,
    ]);
    expect(hierarchy.parentByKey.size).toBe(0);
  });
});

describe("hierarchy visibility and malformed relationships", () => {
  it("hides descendants on collapse and retains a nested collapse when the outer group reopens", () => {
    const nested = { ...worker, id: "nested", coordinatorThreadId: worker.id };
    const sibling = { ...worker, id: "sibling" };
    const hierarchy = buildSidebarHierarchy([coordinator, worker, nested, sibling, unrelated]);
    const collapsed = new Set([sidebarHierarchyKey(coordinator), sidebarHierarchyKey(worker)]);
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, collapsed)).toEqual([
      coordinator,
      unrelated,
    ]);
    collapsed.delete(sidebarHierarchyKey(coordinator));
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, collapsed)).toEqual([
      coordinator,
      worker,
      sibling,
      unrelated,
    ]);
    collapsed.delete(sidebarHierarchyKey(worker));
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, collapsed)).toEqual([
      coordinator,
      worker,
      nested,
      sibling,
      unrelated,
    ]);
  });

  it("expands only supplied shelf/page roots without rendering another group's workers", () => {
    const otherWorker = { ...worker, id: "other-worker", coordinatorThreadId: unrelated.id };
    const hierarchy = buildSidebarHierarchy([coordinator, worker, unrelated, otherWorker]);
    expect(expandSidebarHierarchy([unrelated], hierarchy.childrenByKey, new Set())).toEqual([
      unrelated,
      otherWorker,
    ]);
    expect(expandSidebarHierarchy([], hierarchy.childrenByKey, new Set())).toEqual([]);
  });

  it("ignores stale collapse preferences for unavailable groups", () => {
    const hierarchy = buildSidebarHierarchy([coordinator, worker]);
    expect(
      expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set(["local:deleted"])),
    ).toEqual([coordinator, worker]);
  });

  it("keeps a self-referencing thread visible as a root with its legitimate workers", () => {
    const self = { ...coordinator, coordinatorThreadId: coordinator.id };
    const hierarchy = buildSidebarHierarchy([worker, self]);
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set())).toEqual([
      self,
      worker,
    ]);
    expect(hierarchy.parentByKey.has(sidebarHierarchyKey(self))).toBe(false);
    expect(hierarchy.depthByKey.get(sidebarHierarchyKey(self))).toBe(0);
  });

  it.each([false, true])(
    "breaks cyclic parent links without losing descendants, descendant-first=%s",
    (descendantFirst) => {
      const a = { id: "a", environmentId: "local", coordinatorThreadId: "b" };
      const b = { id: "b", environmentId: "local", coordinatorThreadId: "c" };
      const c = { id: "c", environmentId: "local", coordinatorThreadId: "a" };
      const child = { id: "child", environmentId: "local", coordinatorThreadId: "b" };
      const roster = descendantFirst ? [child, a, b, c, unrelated] : [a, b, c, child, unrelated];
      const hierarchy = buildSidebarHierarchy(roster);
      const displayed = expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set());
      expect(displayed).toEqual([a, b, child, c, unrelated]);
      expect(new Set(displayed.map(sidebarHierarchyKey)).size).toBe(roster.length);
      for (const member of [a, b, c]) {
        expect(hierarchy.parentByKey.has(sidebarHierarchyKey(member))).toBe(false);
        expect(hierarchy.depthByKey.get(sidebarHierarchyKey(member))).toBe(0);
      }
      expect(hierarchy.rootByKey.get(sidebarHierarchyKey(child))).toBe(sidebarHierarchyKey(b));
    },
  );

  it("handles a two-thread cycle as ordinary visible roots", () => {
    const parent = { ...coordinator, coordinatorThreadId: worker.id };
    const hierarchy = buildSidebarHierarchy([worker, parent]);
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set())).toEqual([
      worker,
      parent,
    ]);
    expect(hierarchy.parentByKey.size).toBe(0);
  });

  it("handles an empty roster", () => {
    const hierarchy = buildSidebarHierarchy([]);
    expect(expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set())).toEqual([]);
    expect(hierarchy.rootByKey.size).toBe(0);
    expect(hierarchy.depthByKey.size).toBe(0);
  });
});

describe("explicit search selection reveals coordinator ancestors", () => {
  it("reveals a collapsed worker on repeated selection of the same thread", () => {
    const hierarchy = buildSidebarHierarchy([coordinator, worker, unrelated]);
    const selectedThreadKey = sidebarHierarchyKey(worker);
    const collapsed = [sidebarHierarchyKey(coordinator), sidebarHierarchyKey(unrelated)];
    const visible = (keys: readonly string[]) =>
      expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set(keys)).map(
        sidebarHierarchyKey,
      );
    expect(visible(collapsed)).not.toContain(selectedThreadKey);

    const revealed = revealSidebarThreadAncestors(
      selectedThreadKey,
      hierarchy.parentByKey,
      collapsed,
    );
    expect(visible(revealed)).toContain(selectedThreadKey);
    expect(revealed).toEqual([sidebarHierarchyKey(unrelated)]);

    // Manual collapse followed by selecting the same active worker must reveal it again.
    const recollapsed = [...revealed, sidebarHierarchyKey(coordinator)];
    expect(visible(recollapsed)).not.toContain(selectedThreadKey);
    expect(
      visible(revealSidebarThreadAncestors(selectedThreadKey, hierarchy.parentByKey, recollapsed)),
    ).toContain(selectedThreadKey);
  });

  it("preserves manual collapse while rebuilding and rendering the same route", () => {
    const collapsed = [sidebarHierarchyKey(coordinator)];
    for (const title of ["First update", "Another update"]) {
      const hierarchy = buildSidebarHierarchy([coordinator, { ...worker, title }, unrelated]);
      expect(
        expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set(collapsed)).map(
          sidebarHierarchyKey,
        ),
      ).toEqual([sidebarHierarchyKey(coordinator), sidebarHierarchyKey(unrelated)]);
    }
    expect(collapsed).toEqual([sidebarHierarchyKey(coordinator)]);
  });

  it("expands every nested ancestor while retaining the selected thread's own collapse state", () => {
    const nestedWorker = {
      id: "nested-worker",
      environmentId: "local",
      coordinatorThreadId: worker.id,
    };
    const hierarchy = buildSidebarHierarchy([coordinator, worker, nestedWorker, unrelated]);
    const collapsed = [coordinator, worker, nestedWorker, unrelated].map(sidebarHierarchyKey);
    const revealed = revealSidebarThreadAncestors(
      sidebarHierarchyKey(nestedWorker),
      hierarchy.parentByKey,
      collapsed,
    );
    expect(revealed).toEqual([sidebarHierarchyKey(nestedWorker), sidebarHierarchyKey(unrelated)]);
    expect(
      expandSidebarHierarchy(hierarchy.roots, hierarchy.childrenByKey, new Set(revealed)).map(
        sidebarHierarchyKey,
      ),
    ).toEqual([coordinator, worker, nestedWorker, unrelated].map(sidebarHierarchyKey));
    expect(collapsed).toEqual(
      [coordinator, worker, nestedWorker, unrelated].map(sidebarHierarchyKey),
    );
  });

  it("does not change collapse preferences for root or missing search results", () => {
    const hierarchy = buildSidebarHierarchy([coordinator, worker]);
    const collapsed = [sidebarHierarchyKey(coordinator)];
    expect(
      revealSidebarThreadAncestors(
        sidebarHierarchyKey(coordinator),
        hierarchy.parentByKey,
        collapsed,
      ),
    ).toBe(collapsed);
    expect(revealSidebarThreadAncestors("local:missing", hierarchy.parentByKey, collapsed)).toBe(
      collapsed,
    );
  });

  it("only reveals ancestors in the selected worker's environment", () => {
    const remoteCoordinator = { ...coordinator, environmentId: "remote" };
    const remoteWorker = { ...worker, environmentId: "remote" };
    const hierarchy = buildSidebarHierarchy([coordinator, worker, remoteCoordinator, remoteWorker]);
    const collapsed = [coordinator, remoteCoordinator].map(sidebarHierarchyKey);
    expect(
      revealSidebarThreadAncestors(
        sidebarHierarchyKey(remoteWorker),
        hierarchy.parentByKey,
        collapsed,
      ),
    ).toEqual([sidebarHierarchyKey(coordinator)]);
  });
});
