export const PATTERN_CACHE = new Map<string, RegExp>() ;

export function compilePattern(pattern: string): RegExp {
  if (PATTERN_CACHE.has(pattern)) {
    return PATTERN_CACHE.get(pattern)!;
  }
  
  let regexPattern: string;
  if (pattern === '*') {
    regexPattern = '^.*$';
  } else if (pattern === '**') {
    regexPattern = '^.*$';
  } else if (pattern.includes('**')) {
    regexPattern = '^' + pattern.replace(/\*\*/g, '.*').replace(/\*/g, '[^.]*') + '$';
  } else {
    regexPattern = '^' + pattern.replace(/\*/g, '[^.]*') + '$';
  }
  
  const regex = new RegExp(regexPattern);
  PATTERN_CACHE.set(pattern, regex);
  return regex;
}
