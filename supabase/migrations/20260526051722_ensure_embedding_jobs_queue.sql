do $$
begin
  if to_regclass('pgmq.q_embedding_jobs') is null then
    perform pgmq.create('embedding_jobs');
  end if;
end
$$;
