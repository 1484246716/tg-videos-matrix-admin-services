import { beforeEach, describe, expect, it, vi } from 'vitest';

const {
  contentTagFindMany,
  channelDefaultTagFindMany,
  mediaAssetTagCreateMany,
  mediaAssetTaggingRunCreate,
  generateTextWithAiProfile,
  add,
  info,
  warn,
  logError,
} = vi.hoisted(() => ({
  contentTagFindMany: vi.fn(),
  channelDefaultTagFindMany: vi.fn(),
  mediaAssetTagCreateMany: vi.fn(),
  mediaAssetTaggingRunCreate: vi.fn(),
  generateTextWithAiProfile: vi.fn(),
  add: vi.fn(),
  info: vi.fn(),
  warn: vi.fn(),
  logError: vi.fn(),
}));

vi.mock('../infra/prisma', () => ({
  prisma: {
    contentTag: {
      findMany: contentTagFindMany,
    },
    channelDefaultTag: {
      findMany: channelDefaultTagFindMany,
    },
    mediaAssetTag: {
      createMany: mediaAssetTagCreateMany,
    },
    mediaAssetTaggingRun: {
      create: mediaAssetTaggingRunCreate,
    },
  },
}));

vi.mock('../ai-provider', () => ({
  generateTextWithAiProfile,
}));

vi.mock('../infra/redis', () => ({
  searchIndexQueue: {
    add,
  },
}));

vi.mock('../logger', () => ({
  logger: {
    info,
    warn,
  },
  logError,
}));

import { assignContentTagsForTypeB } from './typeb-content-tag.service';

describe('typeb-content-tag.service', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    contentTagFindMany.mockResolvedValue([
      { id: BigInt(1), name: '角色扮演', slug: 'roleplay-scene', sort: 10, status: 'active', scope: 'adult_18' },
      { id: BigInt(2), name: 'OL', slug: 'tag-ol', sort: 20, status: 'active', scope: 'adult_18' },
      { id: BigInt(3), name: '黑丝', slug: 'black-stockings', sort: 30, status: 'active', scope: 'adult_18' },
    ]);
    channelDefaultTagFindMany.mockResolvedValue([
      {
        createdAt: new Date('2026-04-24T00:00:00.000Z'),
        tag: { id: BigInt(1), name: '角色扮演', slug: 'roleplay-scene', sort: 10, status: 'active', scope: 'adult_18' },
      },
    ]);
    mediaAssetTagCreateMany.mockResolvedValue({ count: 2 });
    mediaAssetTaggingRunCreate.mockResolvedValue({ id: BigInt(1) });
    add.mockResolvedValue(undefined);
  });

  it('appends AI-selected tags on top of base tags when confidence is high', async () => {
    generateTextWithAiProfile.mockResolvedValue(
      JSON.stringify({
        selected_tag_ids: [2, 3],
        confidence: 0.91,
        reason: '命中 OL 和黑丝关键词',
      }),
    );

    const result = await assignContentTagsForTypeB({
      mediaAssetId: BigInt(11),
      channelId: BigInt(22),
      originalName: 'OL黑丝角色扮演',
      aiCaption: '办公室女郎黑丝文案',
      sourceMeta: { sourceChannelUsername: 'office-lady' },
      profile: {
        endpointUrl: null,
        apiKeyEncrypted: 'token',
        model: 'gpt-test',
        temperature: null,
        topP: null,
        maxTokens: null,
      },
      enqueueSearchIndex: true,
    });

    expect(result.appliedTagIds.map(String)).toEqual(['1', '2', '3']);
    expect(mediaAssetTagCreateMany).toHaveBeenCalledWith({
      data: [
        { mediaAssetId: BigInt(11), tagId: BigInt(1) },
        { mediaAssetId: BigInt(11), tagId: BigInt(2) },
        { mediaAssetId: BigInt(11), tagId: BigInt(3) },
      ],
      skipDuplicates: true,
    });
    expect(add).toHaveBeenCalledTimes(1);
    expect(mediaAssetTaggingRunCreate).toHaveBeenCalledTimes(1);
  });

  it('keeps only base tags when AI confidence is too low', async () => {
    generateTextWithAiProfile.mockResolvedValue(
      JSON.stringify({
        selected_tag_ids: [2],
        confidence: 0.42,
        reason: '命中 OL 关键词但把握不足',
      }),
    );

    const result = await assignContentTagsForTypeB({
      mediaAssetId: BigInt(11),
      channelId: BigInt(22),
      originalName: '角色扮演视频',
      aiCaption: 'OL风格文案',
      sourceMeta: {},
      profile: {
        endpointUrl: null,
        apiKeyEncrypted: 'token',
        model: 'gpt-test',
        temperature: null,
        topP: null,
        maxTokens: null,
      },
    });

    expect(result.status).toBe('partial');
    expect(result.appliedTagIds.map(String)).toEqual(['1']);
    expect(mediaAssetTagCreateMany).toHaveBeenCalledWith({
      data: [{ mediaAssetId: BigInt(11), tagId: BigInt(1) }],
      skipDuplicates: true,
    });
    expect(add).not.toHaveBeenCalled();
  });
});
