export type PermissionAction = {
  key: string;
  name: string;
};

export type PermissionModule = {
  key: string;
  name: string;
  actions: PermissionAction[];
};

export const PERMISSION_MODULES: PermissionModule[] = [
  {
    key: "dashboard",
    name: "控制台",
    actions: [{ key: "view", name: "查看" }],
  },
  {
    key: "channels",
    name: "频道管理",
    actions: [
      { key: "view", name: "查看" },
      { key: "create", name: "新增" },
      { key: "update", name: "编辑" },
      { key: "delete", name: "删除" },
    ],
  },
  {
    key: "media",
    name: "视频管理",
    actions: [
      { key: "view", name: "查看" },
      { key: "upload", name: "上传" },
      { key: "update", name: "编辑" },
      { key: "delete", name: "删除" },
    ],
  },
  {
    key: "media-lifecycle",
    name: "视频全链路",
    actions: [
      { key: "view", name: "查看" },
      { key: "update", name: "编辑" },
      { key: "delete", name: "删除" },
    ],
  },
  {
    key: "mass-messaging",
    name: "自定义消息管理",
    actions: [
      { key: "view", name: "查看" },
      { key: "create", name: "新增" },
      { key: "update", name: "编辑" },
      { key: "delete", name: "删除" },
    ],
  },
  {
    key: "tasks",
    name: "任务管理",
    actions: [
      { key: "view", name: "查看" },
      { key: "create", name: "发布任务" },
      { key: "update", name: "编辑任务" },
      { key: "delete", name: "删除任务" },
    ],
  },
  {
    key: "bots",
    name: "机器人管理",
    actions: [
      { key: "view", name: "查看" },
      { key: "create", name: "新增" },
      { key: "update", name: "编辑" },
      { key: "delete", name: "删除" },
    ],
  },
  {
    key: "collections",
    name: "合集管理",
    actions: [
      { key: "view", name: "查看" },
      { key: "create", name: "新增" },
      { key: "update", name: "编辑" },
      { key: "delete", name: "删除" },
    ],
  },
  {
    key: "relay-channels",
    name: "中转私密频道管理",
    actions: [
      { key: "view", name: "查看" },
      { key: "update", name: "编辑" },
      { key: "delete", name: "删除" },
    ],
  },
  {
    key: "users",
    name: "用户管理",
    actions: [
      { key: "view", name: "查看" },
      { key: "create", name: "新增" },
      { key: "update", name: "编辑" },
      { key: "delete", name: "删除" },
    ],
  },
  {
    key: "roles",
    name: "角色与权限",
    actions: [
      { key: "view", name: "查看" },
      { key: "assign", name: "分配权限" },
    ],
  },
  {
    key: "risk-events",
    name: "风控事件",
    actions: [{ key: "view", name: "查看" }],
  },
];

export type RoleTemplate = {
  key: string;
  name: string;
  description: string;
  permissions: string[];
};

const permissionsFor = (moduleKey: string, actions: string[]) =>
  actions.map((action) => `${moduleKey}:${action}`);

export const ROLE_TEMPLATES: RoleTemplate[] = [
  {
    key: "admin",
    name: "超级管理员",
    description: "拥有系统全部权限。",
    permissions: PERMISSION_MODULES.flatMap((mod) =>
      permissionsFor(mod.key, mod.actions.map((a) => a.key)),
    ),
  },
  {
    key: "operator",
    name: "运营专员",
    description: "可查看和管理业务运营相关模块，不含用户/角色管理。",
    permissions: [
      ...permissionsFor("dashboard", ["view"]),
      ...permissionsFor("channels", ["view", "create", "update", "delete"]),
      ...permissionsFor("media", ["view", "upload", "update", "delete"]),
      ...permissionsFor("media-lifecycle", ["view", "update", "delete"]),
      ...permissionsFor("mass-messaging", ["view", "create", "update", "delete"]),
      ...permissionsFor("collections", ["view", "create", "update", "delete"]),
      ...permissionsFor("bots", ["view", "create", "update"]),
      ...permissionsFor("relay-channels", ["view", "update"]),
    ],
  },
  {
    key: "viewer",
    name: "只读成员",
    description: "只具备查看权限。",
    permissions: [
      ...permissionsFor("dashboard", ["view"]),
      ...permissionsFor("channels", ["view"]),
      ...permissionsFor("media", ["view"]),
      ...permissionsFor("media-lifecycle", ["view"]),
      ...permissionsFor("mass-messaging", ["view"]),
      ...permissionsFor("tasks", ["view"]),
      ...permissionsFor("collections", ["view"]),
      ...permissionsFor("bots", ["view"]),
      ...permissionsFor("relay-channels", ["view"]),
    ],
  },
];

export const DEFAULT_ROLE_PERMISSIONS: Record<string, string[]> = {
  admin: ROLE_TEMPLATES.find((t) => t.key === "admin")?.permissions ?? [],
  staff: ROLE_TEMPLATES.find((t) => t.key === "operator")?.permissions ?? [],
};
