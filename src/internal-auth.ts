export interface InternalAuthEnv {
  BWM_INTERNAL_KEY: string;
  BWM_INTERNAL_KEY_NEXT?: string;
}

export function hasInternalKey(key: string, env: InternalAuthEnv): boolean {
	if (typeof env.BWM_INTERNAL_KEY !== "string" || env.BWM_INTERNAL_KEY.length === 0) {
		return false;
	}
  return [env.BWM_INTERNAL_KEY, env.BWM_INTERNAL_KEY_NEXT].some(
    (candidate) =>
      typeof candidate === "string" && candidate.length > 0 && key === candidate,
  );
}
