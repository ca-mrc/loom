import {
  pipelineArtifactFileUrl,
  type PipelineArtifactDetail,
} from "../../api";

export default function GenericArtifactViewer({
  artifact,
}: {
  artifact: PipelineArtifactDetail;
}): JSX.Element {
  return (
    <section aria-labelledby="generic-artifact-files" className="stack-y-3">
      <h2 id="generic-artifact-files" className="text-lg font-semibold">
        Artifact files
      </h2>
      <ul className="stack-divide-y rounded border">
        {artifact.files.map((file) => (
          <li key={file.file_index} className="flex flex-wrap justify-between gap-2 p-3">
            <span>
              <strong>{file.relative_path}</strong> · {file.media_type} · {file.size_bytes} bytes
            </span>
            <a className="text-accent" href={pipelineArtifactFileUrl(artifact.id, file.file_index)}>
              Download
            </a>
          </li>
        ))}
      </ul>
    </section>
  );
}
