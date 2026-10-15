import { describe, expect, it } from 'vitest'
import { splitStepRefs, stepNumbers } from './stepRefs'

// Real step ids: "s_" and 26 characters of the ULID alphabet.
const A = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VP'
const B = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VQ'
const UNKNOWN = 's_11111111111111111111111111'

const numbers = new Map([
  [A, 2],
  [B, 3],
])

describe('splitStepRefs', () => {
  it.each<[string, string, ReturnType<typeof splitStepRefs>]>([
    ['a known id becomes a part with its number', `check ${A} again`, ['check ', { id: A, n: 2 }, ' again']],
    ['an unknown id stays text', `look at ${UNKNOWN}`, [`look at ${UNKNOWN}`]],
    [
      'several ids, known and unknown, keep their order',
      `${A} then ${UNKNOWN} then ${B}`,
      [{ id: A, n: 2 }, ` then ${UNKNOWN} then `, { id: B, n: 3 }],
    ],
    ['an id alone', A, [{ id: A, n: 2 }]],
    ['two ids side by side, with a space', `${A} ${B}`, [{ id: A, n: 2 }, ' ', { id: B, n: 3 }]],
    ['text without an id is one part', 'plain words', ['plain words']],
    ['empty text has no parts', '', []],
    ['an id inside a longer word stays text', `x${A}`, [`x${A}`]],
    ['an id followed by a letter stays text', `${A}x`, [`${A}x`]],
    ['markup-looking text is only text', `<b>${A}</b>`, ['<b>', { id: A, n: 2 }, '</b>']],
  ])('%s', (_name, text, want) => {
    expect(splitStepRefs(text, numbers)).toEqual(want)
  })
})

describe('stepNumbers', () => {
  it('numbers the steps from 1 in the order the room started them', () => {
    const got = stepNumbers([{ id: B }, { id: A }])
    expect(got.get(B)).toBe(1)
    expect(got.get(A)).toBe(2)
    expect(got.get(UNKNOWN)).toBeUndefined()
  })
})
