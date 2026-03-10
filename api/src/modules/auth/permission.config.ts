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
    key: 'dashboard',
    name: '控制台',
    actions: [{ key: 'view', name: '查看' }],
  },
  {
    key: 'users',
    name: '用户管理',
    actions: [
      { key: 'view', name: '查看' },
      { key: 'create', name: '新增' },
      { key: 'update', name: '编辑' },
      { key: 'delete', name: '删除' },
    ],
  },
  {
    key: 'roles',
    name: '角色与权限',
    actions: [
      { key: 'view', name: '查看' },
      { key: 'assign', name: '分配权限' },
    ],
  },
  {
    key: 'videos',
    name: '视频管理',
    actions: [
      { key: 'view', name: '查看' },
      { key: 'upload', name: '上传' },
      { key: 'update', name: '编辑' },
      { key: 'delete', name: '删除' },
    ],
  },
  {
    key: 'tasks',
    name: '任务管理',
    actions: [
      { key: 'view', name: '查看' },
      { key: 'create', name: '发布任务' },
      { key: 'update', name: '编辑任务' },
      { key: 'delete', name: '删除任务' },
    ],
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
    key: 'admin',
    name: '超级管理员',
    description: '拥有系统全部权限。',
    permissions: PERMISSION_MODULES.flatMap((mod) =>
      permissionsFor(mod.key, mod.actions.map((a) => a.key)),
    ),
  },
  {
    key: 'operator',
    name: '运营专员',
    description: '可查看和管理视频与任务，不含用户/角色管理。',
    permissions: [
      ...permissionsFor('dashboard', ['view']),
      ...permissionsFor('videos', ['view', 'upload', 'update', 'delete']),
      ...permissionsFor('tasks', ['view', 'create', 'update', 'delete']),
    ],
  },
  {
    key: 'viewer',
    name: '只读成员',
    description: '只具备查看权限。',
    permissions: [
      ...permissionsFor('dashboard', ['view']),
      ...permissionsFor('videos', ['view']),
      ...permissionsFor('tasks', ['view']),
    ],
  },
];

export const DEFAULT_ROLE_PERMISSIONS: Record<string, string[]> = {
  admin: ROLE_TEMPLATES.find((t) => t.key === 'admin')?.permissions ?? [],
  staff: ROLE_TEMPLATES.find((t) => t.key === 'operator')?.permissions ?? [],
};
