import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  parseJsonObject,
  normalizeKey,
  lowerCompact,
  byProgramGroupFromPath,
  looksSwallowedProgramTitle,
  tsCompact,
  tsCompactMs,
  sha256Short,
  latestJsonlFile,
  chooseSourceJsonl,
} from "./runtime";

// Mock node:fs for filesystem-dependent tests.
// Must be declared at module level so Vitest can hoist it above imports.
vi.mock("node:fs", () => ({
  default: {
    existsSync: vi.fn(),
    readdirSync: vi.fn(),
    statSync: vi.fn(),
  },
  existsSync: vi.fn(),
  readdirSync: vi.fn(),
  statSync: vi.fn(),
}));

import fs from "node:fs";

// ---------------------------------------------------------------------------
// parseJsonObject
// ---------------------------------------------------------------------------
describe("parseJsonObject", () => {
  it("valid object string → object", () => {
    expect(parseJsonObject('{"a":1}')).toEqual({ a: 1 });
  });

  it("array string → null", () => {
    expect(parseJsonObject("[1,2]")).toBeNull();
  });

  it("primitive string → null", () => {
    expect(parseJsonObject('"hello"')).toBeNull();
  });

  it("invalid JSON → null", () => {
    expect(parseJsonObject("not json")).toBeNull();
  });

  it("empty string → null", () => {
    expect(parseJsonObject("")).toBeNull();
  });

  it("non-string input → null", () => {
    expect(parseJsonObject(42)).toBeNull();
    expect(parseJsonObject(null)).toBeNull();
  });

  it("nested object", () => {
    expect(parseJsonObject('{"x":{"y":2}}')).toEqual({ x: { y: 2 } });
  });

  it("whitespace-only string → null", () => {
    expect(parseJsonObject("   ")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// normalizeKey
// ---------------------------------------------------------------------------
describe("normalizeKey", () => {
  it("spaces → underscores", () => {
    expect(normalizeKey("hello world")).toBe("hello_world");
  });

  it("uppercase → lowercase", () => {
    expect(normalizeKey("FooBar")).toBe("foobar");
  });

  it("special chars removed", () => {
    expect(normalizeKey("A<B>C")).toBe("abc");
  });

  it("Windows illegal chars removed", () => {
    expect(normalizeKey('a/b\\c')).toBe("abc");
  });

  it("leading/trailing underscores stripped", () => {
    expect(normalizeKey("_test_")).toBe("test");
  });

  it("multiple spaces collapse to one underscore", () => {
    expect(normalizeKey("a  b")).toBe("a_b");
  });

  it("empty string → empty string", () => {
    expect(normalizeKey("")).toBe("");
  });

  it("already normalized → unchanged", () => {
    expect(normalizeKey("good_key")).toBe("good_key");
  });
});

// ---------------------------------------------------------------------------
// lowerCompact
// ---------------------------------------------------------------------------
describe("lowerCompact", () => {
  it("lowercase and strip spaces", () => {
    expect(lowerCompact("Hello World")).toBe("helloworld");
  });

  it("NFKC normalization: fullwidth → halfwidth", () => {
    // Ａ (U+FF21 fullwidth A) → a after NFKC + lowercase
    expect(lowerCompact("ＡＢＣＤ")).toBe("abcd");
  });

  it("strip special chars", () => {
    expect(lowerCompact("a<b>c")).toBe("abc");
  });

  it("empty string → empty string", () => {
    expect(lowerCompact("")).toBe("");
  });

  it("Japanese chars preserved (no char change, just normalize)", () => {
    expect(lowerCompact("サイエンス")).toBe("サイエンス");
  });

  it("mixed Japanese + special chars", () => {
    expect(lowerCompact("サイエンス<ZERO>")).toBe("サイエンスzero");
  });
});

// ---------------------------------------------------------------------------
// byProgramGroupFromPath
// ---------------------------------------------------------------------------
describe("byProgramGroupFromPath", () => {
  it("by_program segment → next segment", () => {
    expect(byProgramGroupFromPath("B:\\VideoLibrary\\by_program\\NHKスペシャル\\2026\\01\\file.ts"))
      .toBe("NHKスペシャル");
  });

  it("VideoLibrary fallback when no by_program", () => {
    expect(byProgramGroupFromPath("B:\\VideoLibrary\\番組名\\ep.ts")).toBe("番組名");
  });

  it("by_program match is case-insensitive", () => {
    expect(byProgramGroupFromPath("B:\\root\\By_Program\\ShowTitle\\file.ts")).toBe("ShowTitle");
  });

  it("forward slashes", () => {
    expect(byProgramGroupFromPath("B:/VideoLibrary/by_program/ShowX/file.ts")).toBe("ShowX");
  });

  it("no matching segment → undefined", () => {
    expect(byProgramGroupFromPath("B:\\SomeDir\\file.ts")).toBeUndefined();
  });

  it("empty string → undefined", () => {
    expect(byProgramGroupFromPath("")).toBeUndefined();
  });

  it("undefined input → undefined", () => {
    expect(byProgramGroupFromPath(undefined)).toBeUndefined();
  });

  it("by_program is the last segment (no child) → undefined", () => {
    expect(byProgramGroupFromPath("B:\\root\\by_program")).toBeUndefined();
  });

  it("videolibrary case-insensitive fallback", () => {
    expect(byProgramGroupFromPath("B:\\VideoLibrary\\番組名\\2024\\01\\file.ts")).toBe("番組名");
  });
});

// ---------------------------------------------------------------------------
// looksSwallowedProgramTitle
// ---------------------------------------------------------------------------
describe("looksSwallowedProgramTitle", () => {
  it("programTitle 8+ normalized chars longer → true", () => {
    // "サイエンスZERO" = 9 chars (normalized), + " 密着!絶海に浮かぶ奇跡の島" >> 8 chars
    expect(looksSwallowedProgramTitle("サイエンスZERO 密着!絶海に浮かぶ奇跡の島", "サイエンスZERO")).toBe(true);
  });

  it("suffix exactly 8 normalized chars → true (boundary)", () => {
    const folder = "ABCDEF"; // 6 chars
    const program = "ABCDEF12345678"; // 6 + 8 = 14 chars (lowerCompact removes nothing)
    expect(looksSwallowedProgramTitle(program, folder)).toBe(true);
  });

  it("suffix exactly 7 normalized chars → false (boundary)", () => {
    const folder = "ABCDEF"; // 6 chars
    const program = "ABCDEF1234567"; // 6 + 7 = 13 chars
    expect(looksSwallowedProgramTitle(program, folder)).toBe(false);
  });

  it("equal strings → false", () => {
    expect(looksSwallowedProgramTitle("番組名", "番組名")).toBe(false);
  });

  it("programTitle does not start with folderTitle → false", () => {
    expect(looksSwallowedProgramTitle("全然違う番組", "番組名")).toBe(false);
  });

  it("empty programTitle → false", () => {
    expect(looksSwallowedProgramTitle("", "番組名")).toBe(false);
  });

  it("empty folderTitle → false", () => {
    expect(looksSwallowedProgramTitle("番組名 サブタイトル", "")).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// tsCompact
// ---------------------------------------------------------------------------
describe("tsCompact", () => {
  it("formats date as YYYYMMDD_HHMMSS", () => {
    const d = new Date(2026, 2, 27, 15, 30, 42); // March = month 2
    expect(tsCompact(d)).toBe("20260327_153042");
  });

  it("pads single-digit month/day/hour/min/sec", () => {
    const d = new Date(2026, 0, 5, 9, 5, 3); // Jan 5, 09:05:03
    expect(tsCompact(d)).toBe("20260105_090503");
  });

  it("result is always 15 chars", () => {
    const result = tsCompact(new Date());
    expect(result.length).toBe(15);
  });

  it("deterministic for same date", () => {
    const d = new Date(2026, 2, 27, 10, 0, 0);
    expect(tsCompact(d)).toBe(tsCompact(d));
  });
});

// ---------------------------------------------------------------------------
// tsCompactMs
// ---------------------------------------------------------------------------
describe("tsCompactMs", () => {
  it("includes milliseconds", () => {
    const d = new Date(2026, 2, 27, 15, 30, 42, 789);
    expect(tsCompactMs(d)).toBe("20260327_153042_789");
  });

  it("pads single-digit ms with leading zeros", () => {
    const d = new Date(2026, 2, 27, 15, 30, 42, 7);
    expect(tsCompactMs(d)).toBe("20260327_153042_007");
  });

  it("starts with tsCompact result", () => {
    const d = new Date(2026, 2, 27, 10, 0, 0, 500);
    expect(tsCompactMs(d).startsWith(tsCompact(d))).toBe(true);
  });

  it("result is always 19 chars", () => {
    const result = tsCompactMs(new Date());
    expect(result.length).toBe(19);
  });
});

// ---------------------------------------------------------------------------
// sha256Short
// ---------------------------------------------------------------------------
describe("sha256Short", () => {
  it("default length 16", () => {
    const result = sha256Short("hello");
    expect(result.length).toBe(16);
  });

  it("custom length", () => {
    expect(sha256Short("hello", 8).length).toBe(8);
  });

  it("known hash: sha256('abc') starts with ba7816bf", () => {
    // SHA256 of "abc" = ba7816bf8f01cfea414140de5dae2ec73b00361bbef0469ad06d98ae58c76832
    expect(sha256Short("abc", 8)).toBe("ba7816bf");
  });

  it("empty string produces hex of requested length", () => {
    expect(sha256Short("", 16).length).toBe(16);
  });

  it("deterministic: same input → same output", () => {
    expect(sha256Short("テスト")).toBe(sha256Short("テスト"));
  });
});

// ---------------------------------------------------------------------------
// latestJsonlFile (filesystem-dependent, mock fs)
// ---------------------------------------------------------------------------
describe("latestJsonlFile", () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it("directory does not exist → null", () => {
    vi.mocked(fs.existsSync).mockReturnValue(false);
    expect(latestJsonlFile("/some/dir", "prefix_")).toBeNull();
  });

  it("no files matching prefix → null", () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readdirSync as any).mockReturnValue(["other_file.jsonl", "readme.md"]);
    expect(latestJsonlFile("/some/dir", "prefix_")).toBeNull();
  });

  it("single matching file → returns its path", () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readdirSync as any).mockReturnValue(["prefix_001.jsonl"]);
    vi.mocked(fs.statSync as any).mockReturnValue({ mtimeMs: 1000 });
    expect(latestJsonlFile("/some/dir", "prefix_")).toBe("/some/dir/prefix_001.jsonl");
  });

  it("multiple matching files → returns file with highest mtime", () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readdirSync as any).mockReturnValue(["prefix_001.jsonl", "prefix_002.jsonl", "prefix_003.jsonl"]);
    vi.mocked(fs.statSync as any)
      .mockReturnValueOnce({ mtimeMs: 1000 })
      .mockReturnValueOnce({ mtimeMs: 3000 })  // newest
      .mockReturnValueOnce({ mtimeMs: 2000 });
    expect(latestJsonlFile("/some/dir", "prefix_")).toBe("/some/dir/prefix_002.jsonl");
  });
});

// ---------------------------------------------------------------------------
// chooseSourceJsonl (filesystem-dependent, mock fs)
// ---------------------------------------------------------------------------
describe("chooseSourceJsonl", () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it("explicit path exists → ok with that path", () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    const result = chooseSourceJsonl("/llm/dir", "/explicit/path.jsonl");
    expect(result).toEqual({ ok: true, path: "/explicit/path.jsonl" });
  });

  it("explicit path does not exist → error", () => {
    vi.mocked(fs.existsSync).mockReturnValue(false);
    const result = chooseSourceJsonl("/llm/dir", "/missing/path.jsonl");
    expect(result.ok).toBe(false);
    expect(result.error).toContain("/missing/path.jsonl");
  });

  it("no explicit path, latest found → ok with that path", () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readdirSync as any).mockReturnValue(["llm_filename_extract_output_001.jsonl"]);
    vi.mocked(fs.statSync as any).mockReturnValue({ mtimeMs: 1000 });
    const result = chooseSourceJsonl("/llm/dir", undefined);
    expect(result.ok).toBe(true);
    expect(result.path).toContain("llm_filename_extract_output_001.jsonl");
  });

  it("no explicit path, no files found → error", () => {
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readdirSync as any).mockReturnValue([]);
    const result = chooseSourceJsonl("/llm/dir", undefined);
    expect(result.ok).toBe(false);
    expect(result.error).toBeTruthy();
  });
});
