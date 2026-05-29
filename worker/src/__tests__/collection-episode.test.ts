import { describe, expect, it } from 'vitest';
import {
  formatCollectionEpisodeTitle,
  parseEpisodeNoFromText,
  stripEpisodePrefixForTemplate,
} from '../shared/collection-episode';

describe('parseEpisodeNoFromText', () => {
  it('supports the extended 第N<suffix> collection naming set', () => {
    const samples = [
      ['示例 第12集.mp4', 12],
      ['示例 第12期.mp4', 12],
      ['示例 第12回.mp4', 12],
      ['示例 第12季.mp4', 12],
      ['示例 第12话.mp4', 12],
      ['示例 第12部.mp4', 12],
      ['示例 第12幕.mp4', 12],
      ['示例 第12场.mp4', 12],
      ['示例 第12折.mp4', 12],
      ['示例 第12出.mp4', 12],
      ['示例 第12番.mp4', 12],
      ['示例 第12弹.mp4', 12],
      ['示例 第12辑.mp4', 12],
      ['示例 第12篇.mp4', 12],
      ['示例 第12章.mp4', 12],
      ['示例 第12节.mp4', 12],
      ['示例 第12段.mp4', 12],
      ['示例 第12卷.mp4', 12],
      ['示例 第12版.mp4', 12],
      ['示例 第12轮.mp4', 12],
      ['示例 第12档.mp4', 12],
      ['示例 第12战.mp4', 12],
      ['示例 第12案.mp4', 12],
      ['示例 第12宗.mp4', 12],
      ['示例 第12纪.mp4', 12],
      ['示例 第12传.mp4', 12],
      ['示例 第12录.mp4', 12],
      ['示例 第12志.mp4', 12],
      ['示例 第12记.mp4', 12],
      ['示例 第12史.mp4', 12],
      ['示例 第12赋.mp4', 12],
      ['示例 第12歌.mp4', 12],
      ['示例 第12曲.mp4', 12],
      ['示例 第12局.mp4', 12],
      ['示例 第12盘.mp4', 12],
      ['示例 第12赛.mp4', 12],
      ['示例 第12役.mp4', 12],
      ['示例 第12阶.mp4', 12],
      ['示例 第12级.mp4', 12],
      ['示例 第12阵.mp4', 12],
      ['示例 第12波.mp4', 12],
      ['示例 第12次.mp4', 12],
      ['示例 第12序.mp4', 12],
      ['示例 第12楔.mp4', 12],
      ['示例 第12结.mp4', 12],
      ['示例 第12尾.mp4', 12],
      ['示例 第12景.mp4', 12],
      ['示例 第12帧.mp4', 12],
      ['示例 第12轨.mp4', 12],
      ['示例 第12册.mp4', 12],
      ['[第12期] 特辑.mp4', 12],
      ['S01E12.mp4', 12],
    ] as const;

    for (const [input, expected] of samples) {
      expect(parseEpisodeNoFromText(input)).toBe(expected);
    }
  });

  it('supports common traditional variants for the extended suffix set', () => {
    const samples = [
      ['示例 第12話.mp4', 12],
      ['示例 第12場.mp4', 12],
      ['示例 第12彈.mp4', 12],
      ['示例 第12輯.mp4', 12],
      ['示例 第12節.mp4', 12],
      ['示例 第12輪.mp4', 12],
      ['示例 第12檔.mp4', 12],
      ['示例 第12戰.mp4', 12],
      ['示例 第12紀.mp4', 12],
      ['示例 第12傳.mp4', 12],
      ['示例 第12錄.mp4', 12],
      ['示例 第12記.mp4', 12],
      ['示例 第12賦.mp4', 12],
      ['示例 第12盤.mp4', 12],
      ['示例 第12賽.mp4', 12],
      ['示例 第12階.mp4', 12],
      ['示例 第12級.mp4', 12],
      ['示例 第12陣.mp4', 12],
      ['示例 第12幀.mp4', 12],
      ['示例 第12軌.mp4', 12],
      ['示例 第12冊.mp4', 12],
    ] as const;

    for (const [input, expected] of samples) {
      expect(parseEpisodeNoFromText(input)).toBe(expected);
    }
  });

  it('returns null when no supported episode marker exists', () => {
    expect(parseEpisodeNoFromText('示例 第一期.mp4')).toBeNull();
    expect(parseEpisodeNoFromText('示例 EP12.mp4')).toBeNull();
  });

  it('formats collection titles with templateText and removes duplicated episode prefix from source title', () => {
    expect(
      formatCollectionEpisodeTitle({
        collectionName: '黄宏11',
        episodeNo: 1,
        sourceTitle: '第1回《地震》',
        templateText: '第{episodeNo}回 {title}',
      }),
    ).toBe('第1回 《地震》');
  });

  it('strips episode prefixes for the extended suffix set', () => {
    expect(stripEpisodePrefixForTemplate('第1回《地震》', 1)).toBe('《地震》');
    expect(stripEpisodePrefixForTemplate('1期：开场', 1)).toBe('开场');
    expect(stripEpisodePrefixForTemplate('第1集 黄宏11', 1)).toBe('黄宏11');
  });
});
