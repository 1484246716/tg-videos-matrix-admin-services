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
    name: '???',
    actions: [{ key: 'view', name: '??' }],
  },
  {
    key: 'channels',
    name: '????',
    actions: [
      { key: 'view', name: '??' },
      { key: 'create', name: '??' },
      { key: 'update', name: '??' },
      { key: 'delete', name: '??' },
    ],
  },
  {
    key: 'media',
    name: '????',
    actions: [
      { key: 'view', name: '??' },
      { key: 'upload', name: '??' },
      { key: 'update', name: '??' },
      { key: 'delete', name: '??' },
    ],
  },
  {
    key: 'media-lifecycle',
    name: '?????',
    actions: [
      { key: 'view', name: '??' },
      { key: 'update', name: '??' },
      { key: 'delete', name: '??' },
    ],
  },
  {
    key: 'mass-messaging',
    name: '???????',
    actions: [
      { key: 'view', name: '??' },
      { key: 'create', name: '??' },
      { key: 'update', name: '??' },
      { key: 'delete', name: '??' },
    ],
  },
  {
    key: 'tasks',
    name: '????',
    actions: [
      { key: 'view', name: '??' },
      { key: 'create', name: '????' },
      { key: 'update', name: '????' },
      { key: 'delete', name: '????' },
    ],
  },
  {
    key: 'bots',
    name: '?????',
    actions: [
      { key: 'view', name: '??' },
      { key: 'create', name: '??' },
      { key: 'update', name: '??' },
      { key: 'delete', name: '??' },
    ],
  },
  {
    key: 'collections',
    name: '????',
    actions: [
      { key: 'view', name: '??' },
      { key: 'create', name: '??' },
      { key: 'update', name: '??' },
      { key: 'delete', name: '??' },
    ],
  },
  {
    key: 'search',
    name: '??',
    actions: [
      { key: 'view', name: '??' },
      { key: 'manage', name: '??' },
    ],
  },
  {
    key: 'relay-channels',
    name: '????????',
    actions: [
      { key: 'view', name: '??' },
      { key: 'update', name: '??' },
      { key: 'delete', name: '??' },
    ],
  },
  {
    key: 'users',
    name: '????',
    actions: [
      { key: 'view', name: '??' },
      { key: 'create', name: '??' },
      { key: 'update', name: '??' },
      { key: 'delete', name: '??' },
    ],
  },
  {
    key: 'roles',
    name: '?????',
    actions: [
      { key: 'view', name: '??' },
      { key: 'assign', name: '????' },
    ],
  },
  {
    key: 'risk-events',
    name: '????',
    actions: [{ key: 'view', name: '??' }],
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
    name: '?????',
    description: '?????????',
    permissions: PERMISSION_MODULES.flatMap((mod) =>
      permissionsFor(mod.key, mod.actions.map((a) => a.key)),
    ),
  },
  {
    key: 'operator',
    name: '????',
    description: '???????????????????/?????',
    permissions: [
      ...permissionsFor('dashboard', ['view']),
      ...permissionsFor('channels', ['view', 'create', 'update', 'delete']),
      ...permissionsFor('media', ['view', 'upload', 'update', 'delete']),
      ...permissionsFor('media-lifecycle', ['view', 'update', 'delete']),
      ...permissionsFor('mass-messaging', ['view', 'create', 'update', 'delete']),
      ...permissionsFor('tasks', ['view', 'create', 'update', 'delete']),
      ...permissionsFor('collections', ['view', 'create', 'update', 'delete']),
      ...permissionsFor('search', ['view', 'manage']),
      ...permissionsFor('bots', ['view', 'create', 'update']),
      ...permissionsFor('relay-channels', ['view', 'update']),
    ],
  },
  {
    key: 'viewer',
    name: '????',
    description: '????????',
    permissions: [
      ...permissionsFor('dashboard', ['view']),
      ...permissionsFor('channels', ['view']),
      ...permissionsFor('media', ['view']),
      ...permissionsFor('media-lifecycle', ['view']),
      ...permissionsFor('mass-messaging', ['view']),
      ...permissionsFor('tasks', ['view']),
      ...permissionsFor('collections', ['view']),
      ...permissionsFor('search', ['view']),
      ...permissionsFor('bots', ['view']),
      ...permissionsFor('relay-channels', ['view']),
      ...permissionsFor('risk-events', ['view']),
    ],
  },
];

export const DEFAULT_ROLE_PERMISSIONS: Record<string, string[]> = {
  admin: ROLE_TEMPLATES.find((t) => t.key === 'admin')?.permissions ?? [],
  staff: ROLE_TEMPLATES.find((t) => t.key === 'operator')?.permissions ?? [],
};
