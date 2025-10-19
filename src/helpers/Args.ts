const FAST_ARGS: any[][] = [
  [], [undefined], [undefined, undefined], 
  [undefined, undefined, undefined], 
  [undefined, undefined, undefined, undefined],
  [undefined, undefined, undefined, undefined, undefined]
];

export function getArgs(count: number): any[] {
  if (count < FAST_ARGS.length) {
    const args = FAST_ARGS[count].slice();
    args.length = count;
    return args;
  }
  return new Array(count);
}
