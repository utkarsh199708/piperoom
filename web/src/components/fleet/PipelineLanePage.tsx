import { Link, useParams, useSearchParams } from 'react-router-dom'
import { useTopology } from '../../hooks/useTopology'
import { useTrends } from '../../hooks/useTrends'
import { PipelineMap } from '../PipelineMap'
import { Button } from '../ui/button'

// One pipeline's lane outside a room (X5b): where a fleet row and its worst-node link go. `?node=` is the fleet's
// `worst_node`, an id as the API gave it; the lane presses that node's layer when the node is on the lane, and says
// so when it is not (a node the topology does not draw right now). Nothing here writes.
export function PipelineLanePage() {
  const { pipelineId = '' } = useParams()
  const [params] = useSearchParams()
  const nodeId = params.get('node') ?? ''
  const { topology, updatedAt, notFound, retry } = useTopology(pipelineId)
  const trends = useTrends(pipelineId)
  const node = nodeId && topology ? topology.nodes.find((n) => n.id === nodeId) : undefined

  return (
    <div className="flex min-w-0 flex-col gap-4 px-3 py-5 md:px-6">
      <div className="flex flex-wrap items-center gap-2">
        <Button variant="outline" size="sm" asChild>
          <Link to="/fleet">Back to fleet</Link>
        </Button>
        <Button variant="ghost" size="sm" asChild>
          <Link to={`/?pipeline=${encodeURIComponent(pipelineId)}`}>Rooms on this pipeline</Link>
        </Button>
      </div>
      {nodeId && topology && (
        <p className="m-0 text-sm text-muted-foreground" data-testid="worst-node">
          Worst node: <span className="font-mono text-foreground">{nodeId}</span>
          {node ? ` (${node.layer})` : ' (not on the lane right now)'}
        </p>
      )}
      <PipelineMap
        topology={topology}
        updatedAt={updatedAt}
        notFound={notFound}
        onRetry={retry}
        pipelineId={pipelineId}
        selectedLayer={node?.layer}
        trends={trends.series}
      />
    </div>
  )
}
