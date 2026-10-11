// Dev users, mirroring dev/users.yaml. The header switcher lets two tabs act as
// two people. In production the identity would come from GET /api/me.
export interface DevUser {
  id: string
  name: string
  layers: string[]
}

export const DEV_USERS: DevUser[] = [
  { id: 'arjun', name: 'Arjun', layers: ['kafka-connect', 'kafka', 'schema-registry'] },
  { id: 'priya', name: 'Priya', layers: ['postgres'] },
  { id: 'meera', name: 'Meera', layers: ['warehouse'] },
  { id: 'kabir', name: 'Kabir', layers: ['consumer'] },
]

export const DEFAULT_USER_ID = 'arjun'

export function findUser(id: string | null | undefined): DevUser | undefined {
  return DEV_USERS.find((u) => u.id === id)
}
