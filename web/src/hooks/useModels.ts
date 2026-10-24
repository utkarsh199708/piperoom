import { api } from '../api/client'
import { useLoad } from './useLoad'

const loadModels = () => api.models()

/** GET /api/models (any logged-in user), with a retry. */
export function useModels() {
  return useLoad(loadModels)
}
