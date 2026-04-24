import { describe, expect, it } from 'vitest';
import { buildAdultContentTagRecall } from './typeb-content-tag-rules.service';

describe('typeb-content-tag-rules.service', () => {
  const activeTags = [
    { id: BigInt(1), name: 'OL', slug: 'tag-ol', sort: 10, status: 'active', scope: 'adult_18' },
    { id: BigInt(2), name: '黑丝', slug: 'black-stockings', sort: 20, status: 'active', scope: 'adult_18' },
    { id: BigInt(3), name: '无套中出', slug: 'finish-inside-without-condom', sort: 30, status: 'active', scope: 'adult_18' },
    { id: BigInt(4), name: '角色扮演', slug: 'roleplay-scene', sort: 40, status: 'active', scope: 'adult_18' },
    { id: BigInt(5), name: '剧情', slug: 'storyline', sort: 50, status: 'active', scope: 'adult_18' },
  ] as const;

  it('marks channel defaults and strong text matches as base tags', () => {
    const result = buildAdultContentTagRecall({
      activeTags: [...activeTags],
      channelDefaultTags: [{ id: BigInt(4), name: '角色扮演', slug: 'roleplay-scene' }],
      originalName: 'OL黑丝 无套中出',
      aiCaption: '办公室女郎角色扮演',
      sourceMeta: {
        sourceChannelUsername: 'office-lady',
        messageText: '黑丝OL 无套中出',
      },
    });

    expect(result.baseTagIds.map(String)).toEqual(expect.arrayContaining(['1', '2', '3', '4']));
    expect(result.candidateTagIds.map(String)).toEqual(expect.arrayContaining(['1', '2', '3', '4']));
    expect(result.candidates.find((item) => item.tagId === BigInt(1))?.matchedSources).toEqual(
      expect.arrayContaining(['original_name', 'source_channel', 'source_caption']),
    );
  });

  it('keeps AI-caption-only hits as candidates without promoting them to base tags', () => {
    const result = buildAdultContentTagRecall({
      activeTags: [...activeTags],
      channelDefaultTags: [],
      originalName: '普通视频',
      aiCaption: '这是一段剧情向角色扮演文案',
      sourceMeta: {},
    });

    expect(result.candidateTagIds.map(String)).toEqual(expect.arrayContaining(['4', '5']));
    expect(result.baseTagIds.map(String)).not.toContain('5');
  });
});
