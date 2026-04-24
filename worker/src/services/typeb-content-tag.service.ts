/**
 * 内容标签服务: 负责为 TypeB 资源分配 AI 内容标签。
 * 调用链路: dispatch.service -> assignContentTagsForTypeB -> 更新数据库相关表。
 */

import { AiModelProfile, ContentTag } from '@prisma/client';
import { generateTextWithAiProfile } from '../ai-provider';
import { prisma } from '../infra/prisma';
import { searchIndexQueue } from '../infra/redis';
import { logger, logError } from '../logger';
import { buildTypeBContentTagPrompts } from './typeb-content-tag-prompt';
import {
  ADULT_CONTENT_TAG_SCOPE,
  buildAdultContentTagRecall,
  DEFAULT_CONTENT_TAG_CANDIDATE_LIMIT,
} from './typeb-content-tag-rules.service';

const MIN_AI_CONFIDENCE = 0.65;
const MAX_SELECTED_TAGS = 5;

type AdultContentTagRecord = Pick<ContentTag, 'id' | 'name' | 'slug' | 'sort' | 'status' | 'scope'>;
type TaggingProfile = Pick<
  AiModelProfile,
  'endpointUrl' | 'apiKeyEncrypted' | 'model' | 'temperature' | 'topP' | 'maxTokens'
>;

type AiSelectionParseResult = {
  selectedTagIds: bigint[];
  rejectedTagIds: bigint[];
  confidence: number | null;
  reason: string | null;
};

export type AssignContentTagsForTypeBResult = {
  ok: true;
  status: 'success' | 'partial' | 'skipped';
  mediaAssetId: bigint;
  candidateTagIds: bigint[];
  baseTagIds: bigint[];
  aiTagIds: bigint[];
  appliedTagIds: bigint[];
  confidence: number | null;
  reason: string | null;
};

// 为 TypeB 资源异步分配 AI 内容标签
export async function assignContentTagsForTypeB(args: {
  mediaAssetId: bigint;
  channelId: bigint;
  originalName: string;
  aiCaption: string | null;
  sourceMeta: unknown;
  profile?: TaggingProfile | null;
  triggerSource?: string;
  candidateLimit?: number;
  enqueueSearchIndex?: boolean;
}): Promise<AssignContentTagsForTypeBResult> {
  const triggerSource = normalizeTriggerSource(args.triggerSource);
  const activeTags = await loadActiveAdultTags();
  const channelDefaultTags = await loadChannelDefaultTags(args.channelId);
  const recall = buildAdultContentTagRecall({
    activeTags,
    channelDefaultTags,
    originalName: args.originalName,
    aiCaption: args.aiCaption,
    sourceMeta: args.sourceMeta,
    candidateLimit: args.candidateLimit ?? DEFAULT_CONTENT_TAG_CANDIDATE_LIMIT,
  });

  const activeTagById = new Map(activeTags.map((tag) => [tag.id.toString(), tag]));
  const baseTagIds = recall.baseTagIds.filter((tagId) => activeTagById.has(tagId.toString()));
  const baseTags = baseTagIds
    .map((tagId) => activeTagById.get(tagId.toString()))
    .filter((tag): tag is AdultContentTagRecord => Boolean(tag));

  let aiTagIds: bigint[] = [];
  let aiConfidence: number | null = null;
  let aiReason: string | null = null;
  let aiRejectedTagIds: bigint[] = [];
  let aiRaw: string | null = null;
  let status: AssignContentTagsForTypeBResult['status'] = 'success';
  let finalReason: string | null = null;

  try {
    if (args.profile && recall.candidates.length > 0) {
      const prompts = buildTypeBContentTagPrompts({
        originalName: args.originalName,
        aiCaption: args.aiCaption,
        sourceChannel: recall.sourceSnapshot.sourceChannel,
        sourceCaption: recall.sourceSnapshot.sourceCaption,
        baseTags: baseTags.map((tag) => ({ id: tag.id, name: tag.name })),
        candidates: recall.candidates,
      });

      aiRaw = await generateTextWithAiProfile(args.profile, prompts.systemPrompt, prompts.userPrompt);
      const parsed = parseAiSelection(aiRaw);
      const validatedTagIds = validateAiSelectedTagIds({
        selectedTagIds: parsed.selectedTagIds,
        candidateTagIds: recall.candidateTagIds,
        activeTags,
      });

      aiConfidence = parsed.confidence;
      aiReason = parsed.reason;
      aiRejectedTagIds = parsed.rejectedTagIds;

      if (validatedTagIds.length > 0 && aiConfidence !== null && aiConfidence >= MIN_AI_CONFIDENCE) {
        aiTagIds = validatedTagIds;
      } else if (validatedTagIds.length > 0 && baseTagIds.length > 0) {
        status = 'partial';
        finalReason = 'base_tags_only_low_confidence';
      } else if (validatedTagIds.length === 0 && baseTagIds.length > 0) {
        status = 'partial';
        finalReason = 'base_tags_only_invalid_ai_selection';
      } else {
        status = 'skipped';
        finalReason = validatedTagIds.length === 0 ? 'invalid_ai_selection' : 'low_confidence';
      }
    } else if (baseTagIds.length > 0) {
      status = 'partial';
      finalReason = args.profile ? 'base_tags_only_no_candidates' : 'base_tags_only_no_profile';
    } else {
      status = 'skipped';
      finalReason = args.profile ? 'no_candidate_tags' : 'no_profile_and_no_rule_tags';
    }

    const appliedTagIds = dedupeBigInt([...baseTagIds, ...aiTagIds]);

    if (appliedTagIds.length > 0) {
      await prisma.mediaAssetTag.createMany({
        data: appliedTagIds.map((tagId) => ({
          mediaAssetId: args.mediaAssetId,
          tagId,
        })),
        skipDuplicates: true,
      });
    }

    if (args.enqueueSearchIndex && appliedTagIds.length > 0) {
      await enqueueSearchRebuild(args.mediaAssetId);
    }

    await writeTaggingRunLog({
      mediaAssetId: args.mediaAssetId,
      channelId: args.channelId,
      triggerSource,
      candidateCount: recall.candidateTagIds.length,
      selectedCount: appliedTagIds.length,
      confidence: aiConfidence,
      status: appliedTagIds.length > 0 ? status : 'skipped',
      reason: finalReason ?? aiReason,
      payload: {
        originalName: args.originalName,
        sourceSnapshot: recall.sourceSnapshot,
        baseTagIds: baseTagIds.map(String),
        candidateTagIds: recall.candidateTagIds.map(String),
        aiTagIds: aiTagIds.map(String),
        appliedTagIds: appliedTagIds.map(String),
        aiRejectedTagIds: aiRejectedTagIds.map(String),
        aiReason,
        aiRaw: truncateText(aiRaw, 4000),
        candidates: recall.candidates.map((candidate) => ({
          tagId: candidate.tagId.toString(),
          name: candidate.name,
          score: candidate.score,
          isStrong: candidate.isStrong,
          matchedSources: candidate.matchedSources,
          matchedKeywords: candidate.matchedKeywords,
        })),
      },
    });

    logger.info('[typeb_content_tag] 自动标签完成', {
      mediaAssetId: args.mediaAssetId.toString(),
      channelId: args.channelId.toString(),
      triggerSource,
      status,
      candidateCount: recall.candidateTagIds.length,
      baseTagCount: baseTagIds.length,
      aiTagCount: aiTagIds.length,
      appliedTagCount: appliedTagIds.length,
      confidence: aiConfidence,
      reason: finalReason ?? aiReason,
    });

    return {
      ok: true,
      status: appliedTagIds.length > 0 ? status : 'skipped',
      mediaAssetId: args.mediaAssetId,
      candidateTagIds: recall.candidateTagIds,
      baseTagIds,
      aiTagIds,
      appliedTagIds,
      confidence: aiConfidence,
      reason: finalReason ?? aiReason,
    };
  } catch (error) {
    logError('[typeb_content_tag] 自动标签失败', {
      mediaAssetId: args.mediaAssetId.toString(),
      channelId: args.channelId.toString(),
      triggerSource,
      error,
    });

    await writeTaggingRunLog({
      mediaAssetId: args.mediaAssetId,
      channelId: args.channelId,
      triggerSource,
      candidateCount: recall.candidateTagIds.length,
      selectedCount: 0,
      confidence: aiConfidence,
      status: 'failed',
      reason: error instanceof Error ? error.message : String(error),
      payload: {
        originalName: args.originalName,
        sourceSnapshot: recall.sourceSnapshot,
        baseTagIds: baseTagIds.map(String),
        candidateTagIds: recall.candidateTagIds.map(String),
        aiRaw: truncateText(aiRaw, 4000),
      },
    });

    throw error;
  }
}

// 加载当前活跃的成人内容标签
async function loadActiveAdultTags(): Promise<AdultContentTagRecord[]> {
  return prisma.contentTag.findMany({
    where: {
      status: 'active',
      scope: ADULT_CONTENT_TAG_SCOPE,
    },
    orderBy: [{ sort: 'asc' }, { name: 'asc' }],
  });
}

// 加载频道默认标签
async function loadChannelDefaultTags(channelId: bigint) {
  const rows = await prisma.channelDefaultTag.findMany({
    where: { channelId },
    include: { tag: true },
    orderBy: { createdAt: 'asc' },
  });

  return rows
    .map((row) => row.tag)
    .filter((tag) => tag.status === 'active' && tag.scope === ADULT_CONTENT_TAG_SCOPE)
    .map((tag) => ({
      id: tag.id,
      name: tag.name,
      slug: tag.slug,
    }));
}

// 解析 AI 选择结果
function parseAiSelection(raw: string): AiSelectionParseResult {
  const jsonText = extractJsonBlock(raw);
  const parsed = JSON.parse(jsonText) as {
    selected_tag_ids?: unknown;
    rejected_tag_ids?: unknown;
    confidence?: unknown;
    reason?: unknown;
  };

  return {
    selectedTagIds: normalizeBigIntArray(parsed.selected_tag_ids),
    rejectedTagIds: normalizeBigIntArray(parsed.rejected_tag_ids),
    confidence: normalizeConfidence(parsed.confidence),
    reason: typeof parsed.reason === 'string' && parsed.reason.trim() ? parsed.reason.trim() : null,
  };
}

// 从 AI 返回文本中提取 JSON 块
function extractJsonBlock(raw: string) {
  const trimmed = raw.trim();
  const fencedMatch = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fencedMatch?.[1]) return fencedMatch[1].trim();

  const objectMatch = trimmed.match(/\{[\s\S]*\}/);
  if (objectMatch?.[0]) return objectMatch[0];

  return trimmed;
}

// 校验 AI 选择的标签 ID 是否合法（必须在候选列表中且为活跃状态）
function validateAiSelectedTagIds(args: {
  selectedTagIds: bigint[];
  candidateTagIds: bigint[];
  activeTags: AdultContentTagRecord[];
}) {
  const candidateSet = new Set(args.candidateTagIds.map((tagId) => tagId.toString()));
  const activeSet = new Set(args.activeTags.map((tag) => tag.id.toString()));

  return dedupeBigInt(args.selectedTagIds)
    .filter((tagId) => candidateSet.has(tagId.toString()) && activeSet.has(tagId.toString()))
    .slice(0, MAX_SELECTED_TAGS);
}

// 归一化 BigInt 数组
function normalizeBigIntArray(value: unknown) {
  if (!Array.isArray(value)) return [];
  return dedupeBigInt(
    value
      .map((item) => {
        if (typeof item === 'bigint') return item;
        if (typeof item === 'number' && Number.isFinite(item) && item > 0) return BigInt(Math.floor(item));
        if (typeof item === 'string' && /^\d+$/.test(item.trim())) return BigInt(item.trim());
        return null;
      })
      .filter((item): item is bigint => Boolean(item)),
  );
}

// 归一化置信度分数
function normalizeConfidence(value: unknown) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return clampConfidence(value);
  }

  if (typeof value === 'string' && value.trim()) {
    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
      return clampConfidence(numeric);
    }
  }

  return null;
}

// 限制置信度在 0-1 之间
function clampConfidence(value: number) {
  if (value < 0) return 0;
  if (value > 1) return 1;
  return Number(value.toFixed(4));
}

// 将搜索索引重建任务加入队列
async function enqueueSearchRebuild(mediaAssetId: bigint) {
  await searchIndexQueue.add(
    'upsert',
    {
      sourceType: 'media_asset',
      sourceId: mediaAssetId.toString(),
    },
    {
      jobId: `search-index-tags-asset-${mediaAssetId.toString()}-${Date.now()}`,
    },
  );
}

// 记录标签运行日志
async function writeTaggingRunLog(args: {
  mediaAssetId: bigint;
  channelId: bigint;
  triggerSource: string;
  candidateCount: number;
  selectedCount: number;
  confidence: number | null;
  status: string;
  reason: string | null;
  payload: Record<string, unknown>;
}) {
  const model = (prisma as any).mediaAssetTaggingRun;
  if (!model?.create) return;

  try {
    await model.create({
      data: {
        mediaAssetId: args.mediaAssetId,
        channelId: args.channelId,
        triggerSource: args.triggerSource,
        candidateCount: args.candidateCount,
        selectedCount: args.selectedCount,
        confidence: args.confidence,
        status: args.status,
        reason: args.reason,
        payload: args.payload,
      },
    });
  } catch (error) {
    logger.warn('[typeb_content_tag] 执行日志写入失败（忽略）', {
      mediaAssetId: args.mediaAssetId.toString(),
      channelId: args.channelId.toString(),
      triggerSource: args.triggerSource,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

// 归一化触发源名称
function normalizeTriggerSource(value: string | null | undefined) {
  const normalized = String(value || '').trim();
  return normalized || 'dispatch_typeb';
}

// BigInt 数组去重
function dedupeBigInt(values: bigint[]) {
  return Array.from(new Set(values.map((value) => value.toString()))).map((value) => BigInt(value));
}

// 截断超长文本
function truncateText(value: string | null, maxLength: number) {
  if (!value) return null;
  return value.length <= maxLength ? value : `${value.slice(0, maxLength)}...`;
}
