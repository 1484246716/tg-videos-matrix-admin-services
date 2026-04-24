import { AdultContentTagCandidate } from './typeb-content-tag-rules.service';

export const MAX_CONTENT_TAG_SELECTION = 5;

export function buildTypeBContentTagPrompts(args: {
  originalName: string;
  aiCaption: string | null;
  sourceChannel: string | null;
  sourceCaption: string | null;
  baseTags: Array<{ id: bigint; name: string }>;
  candidates: AdultContentTagCandidate[];
}) {
  const systemPrompt = [
    '你是成人视频内容标签选择器。',
    '你只能从给定候选标签中选择，禁止创造新标签，禁止改写标签名称或返回不存在的 tag_id。',
    '如果信息不足，可以返回空数组。',
    '必须输出严格 JSON，禁止输出 Markdown、代码块或额外解释。',
    `最多选择 ${MAX_CONTENT_TAG_SELECTION} 个标签。`,
    '输出 JSON 格式：{"selected_tag_ids":[1,2],"confidence":0.82,"reason":"...","rejected_tag_ids":[3]}',
  ].join('\n');

  const userPrompt = [
    `原始文件名：${args.originalName}`,
    `AI 简介：${args.aiCaption || '无'}`,
    `来源频道：${args.sourceChannel || '无'}`,
    `来源文案：${args.sourceCaption || '无'}`,
    `规则已命中标签：${formatBaseTags(args.baseTags)}`,
    '候选标签列表：',
    ...args.candidates.map(
      (candidate, index) =>
        `${index + 1}. ${candidate.tagId.toString()} | ${candidate.name} | 来源=${candidate.matchedSources.join(',') || 'unknown'}`,
    ),
    '',
    '任务要求：',
    '1. 只返回候选标签里的 tag_id。',
    '2. selected_tag_ids 请按最相关到次相关排序。',
    '3. confidence 取值 0-1。',
    '4. reason 用一句中文解释依据，尽量引用输入中的关键词。',
  ].join('\n');

  return {
    systemPrompt,
    userPrompt,
  };
}

function formatBaseTags(baseTags: Array<{ id: bigint; name: string }>) {
  if (baseTags.length === 0) return '无';
  return baseTags.map((tag) => `${tag.id.toString()}:${tag.name}`).join('、');
}
