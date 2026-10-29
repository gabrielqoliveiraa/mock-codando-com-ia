// Codando com IA — mock interview (cenário fictício inspirado no QuintoAndar)
// INCIDENTE: "confirmações de visita chegando atrasadas no fechamento do mês".
// Executar (JDK 21):  java NotificacoesQuintoAndar.java
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class NotificacoesQuintoAndar {

  // Tipos de trabalho que o worker processa. Duração simulada em ms.
  enum TipoJob {
    RELATORIO_PROPRIETARIO(80), // relatório mensal do proprietário (lento, chega em lote)
    ASSINATURA_CONTRATO(30),    // aviso de contrato pronto para assinar
    CONFIRMACAO_VISITA(5);      // confirmação de visita agendada (rápido, sensível a atraso)

    final int duracaoMs;
    TipoJob(int duracaoMs) { this.duracaoMs = duracaoMs; }
  }

  record Job(long id, TipoJob tipo, String imovelId, long criadoEmNanos) {}

  // ---------- Filas: uma por tipo de job ----------
  static class Filas {
    final Map<TipoJob, ConcurrentLinkedQueue<Job>> porTipo = new EnumMap<>(TipoJob.class);
    Filas() { for (TipoJob t : TipoJob.values()) porTipo.put(t, new ConcurrentLinkedQueue<>()); }
    void publicar(Job j) { porTipo.get(j.tipo()).add(j); }
    boolean vazias() { return porTipo.values().stream().allMatch(Queue::isEmpty); }
  }

  // ---------- Gateway externo (WhatsApp/e-mail). Pode falhar de forma transitória ----------
  static class GatewayNotificacao {
    static final double TAXA_FALHA = 0.0; // cenário 2 do incidente: mude para 0.2
    final AtomicInteger chamadas = new AtomicInteger();
    void enviar(Job j) throws InterruptedException {
      chamadas.incrementAndGet();
      long extra = (j.tipo() == TipoJob.RELATORIO_PROPRIETARIO && j.id() % 10 == 0) ? 250 : 0; // alguns relatórios bem lentos
      Thread.sleep(j.tipo().duracaoMs + extra);
      if (ThreadLocalRandom.current().nextDouble() < TAXA_FALHA) throw new IllegalStateException("gateway indisponível");
    }
  }

  // ---------- Métricas / painel (é o "dashboard" do incidente) ----------
  static class Metricas {
    final Map<TipoJob, List<Long>> esperaMs = new ConcurrentHashMap<>();   // criado -> início do processamento
    final Map<TipoJob, List<Long>> latenciaMs = new ConcurrentHashMap<>(); // criado -> fim (o que o usuário sente)
    final AtomicInteger falhas = new AtomicInteger();
    final AtomicInteger retries = new AtomicInteger();
    final AtomicInteger emProcessamento = new AtomicInteger();
    final AtomicLong tempoOcupadoNanos = new AtomicLong();

    static void add(Map<TipoJob, List<Long>> m, TipoJob t, long v) {
      m.computeIfAbsent(t, k -> Collections.synchronizedList(new ArrayList<>())).add(v);
    }
    void inicio() { emProcessamento.incrementAndGet(); }
    void fim(long inicioNanos) { emProcessamento.decrementAndGet(); tempoOcupadoNanos.addAndGet(System.nanoTime() - inicioNanos); }
    void registrar(Job j, long inicioNanos) {
      add(esperaMs, j.tipo(), (inicioNanos - j.criadoEmNanos()) / 1_000_000);
      add(latenciaMs, j.tipo(), (System.nanoTime() - j.criadoEmNanos()) / 1_000_000);
    }
    static long pct(List<Long> ordenada, double p) { return ordenada.get((int) Math.ceil(ordenada.size() * p) - 1); }

    // Linha do painel ao vivo: backlog por fila e threads ocupadas naquele instante.
    void amostrar(long inicioNanos, Filas filas, int threads) {
      long t = (System.nanoTime() - inicioNanos) / 1_000_000;
      int ocupadas = emProcessamento.get();
      StringBuilder sb = new StringBuilder(String.format("[painel t=%5d ms] threads ocupadas %d/%d | backlog:", t, ocupadas, threads));
      for (TipoJob tipo : TipoJob.values()) sb.append(String.format(" %s=%d", abreviar(tipo), filas.porTipo.get(tipo).size()));
      sb.append(String.format(" | concluídos=%d | retries=%d", latenciaMs.values().stream().mapToInt(List::size).sum(), retries.get()));
      System.out.println(sb);
    }
    static String abreviar(TipoJob t) {
      return switch (t) { case RELATORIO_PROPRIETARIO -> "relatorio"; case ASSINATURA_CONTRATO -> "contrato"; case CONFIRMACAO_VISITA -> "visita"; };
    }

    void imprimirResumo(long inicioNanos, int publicados, int chamadasGateway, int threads) {
      long totalMs = (System.nanoTime() - inicioNanos) / 1_000_000;
      int processados = latenciaMs.values().stream().mapToInt(List::size).sum();
      double utilizacao = tempoOcupadoNanos.get() / 1e6 / (threads * (double) totalMs) * 100;
      System.out.println("================ RESUMO DO PAINEL ================");
      System.out.printf("Tempo total: %d ms | processados: %d/%d | throughput: %.1f jobs/s%n", totalMs, processados, publicados, processados * 1000.0 / totalMs);
      System.out.printf("Utilização média das %d threads: %.0f%% (ociosas %.0f%% do tempo)%n", threads, utilizacao, 100 - utilizacao);
      System.out.printf("Gateway: %d chamadas | retries: %d | falhas definitivas: %d | taxa de falha por chamada: %.1f%%%n",
          chamadasGateway, retries.get(), falhas.get(), chamadasGateway == 0 ? 0 : (retries.get() + falhas.get()) * 100.0 / chamadasGateway);
      System.out.println("Fila                      n   espera p50  espera p95   total p95   total max");
      for (TipoJob t : TipoJob.values()) {
        List<Long> e = new ArrayList<>(esperaMs.getOrDefault(t, List.of()));
        List<Long> l = new ArrayList<>(latenciaMs.getOrDefault(t, List.of()));
        if (l.isEmpty()) continue;
        Collections.sort(e); Collections.sort(l);
        System.out.printf("  %-22s %3d  %8d ms  %8d ms  %8d ms  %8d ms%n", t, l.size(), pct(e, 0.5), pct(e, 0.95), pct(l, 0.95), l.get(l.size() - 1));
      }
    }
  }

  // ---------- Worker: o código a ser investigado ----------
  static class Dispatcher {
    static final int THREADS = 4;
    final ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    final Filas filas; final GatewayNotificacao gateway; final Metricas metricas;
    Dispatcher(Filas f, GatewayNotificacao g, Metricas m) { filas = f; gateway = g; metricas = m; }

    // Pega até THREADS jobs da primeira fila não vazia (na ordem do enum).
    List<Job> proximoLote() {
      for (TipoJob t : TipoJob.values()) {
        Queue<Job> q = filas.porTipo.get(t);
        List<Job> lote = new ArrayList<>();
        Job j;
        while (lote.size() < THREADS && (j = q.poll()) != null) lote.add(j);
        if (!lote.isEmpty()) return lote;
      }
      return List.of();
    }

    void processar(Job j) {
      long inicio = System.nanoTime();
      metricas.inicio();
      try {
        for (int tentativa = 1; tentativa <= 3; tentativa++) {
          try { gateway.enviar(j); metricas.registrar(j, inicio); return; }
          catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
          catch (RuntimeException e) { if (tentativa < 3) metricas.retries.incrementAndGet(); /* tenta de novo imediatamente */ }
        }
        metricas.falhas.incrementAndGet();
      } finally { metricas.fim(inicio); }
    }

    void rodar(AtomicBoolean produtorAtivo) throws InterruptedException {
      while (produtorAtivo.get() || !filas.vazias()) {
        List<Job> lote = proximoLote();
        if (lote.isEmpty()) { Thread.sleep(1); continue; }
        CountDownLatch fimDoLote = new CountDownLatch(lote.size());
        for (Job j : lote) pool.submit(() -> { try { processar(j); } finally { fimDoLote.countDown(); } });
        fimDoLote.await(); // espera o lote inteiro antes de buscar mais
      }
      pool.shutdown();
      pool.awaitTermination(1, TimeUnit.MINUTES);
    }
  }

  // ---------- Simulação de carga (fechamento do mês) ----------
  public static void main(String[] args) throws Exception {
    Filas filas = new Filas();
    GatewayNotificacao gateway = new GatewayNotificacao();
    Metricas metricas = new Metricas();
    AtomicLong ids = new AtomicLong();
    AtomicInteger publicados = new AtomicInteger();
    AtomicBoolean produtorAtivo = new AtomicBoolean(true);
    long inicio = System.nanoTime();

    Thread produtor = new Thread(() -> {
      try {
        for (int i = 0; i < 80; i++) { // fechamento do mês: lote de relatórios de uma vez
          filas.publicar(new Job(ids.incrementAndGet(), TipoJob.RELATORIO_PROPRIETARIO, "imovel-" + i, System.nanoTime()));
          publicados.incrementAndGet();
        }
        for (int tick = 0; tick < 100; tick++) { // ~2 s de tráfego contínuo
          filas.publicar(new Job(ids.incrementAndGet(), TipoJob.CONFIRMACAO_VISITA, "imovel-" + (tick % 30), System.nanoTime()));
          publicados.incrementAndGet();
          if (tick % 5 == 0) {
            filas.publicar(new Job(ids.incrementAndGet(), TipoJob.ASSINATURA_CONTRATO, "imovel-" + (tick % 30), System.nanoTime()));
            publicados.incrementAndGet();
          }
          Thread.sleep(20);
        }
      } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      finally { produtorAtivo.set(false); }
    });

    ScheduledExecutorService painel = Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
    painel.scheduleAtFixedRate(() -> metricas.amostrar(inicio, filas, Dispatcher.THREADS), 0, 500, TimeUnit.MILLISECONDS);

    produtor.start();
    new Dispatcher(filas, gateway, metricas).rodar(produtorAtivo);
    produtor.join();
    painel.shutdownNow();
    metricas.imprimirResumo(inicio, publicados.get(), gateway.chamadas.get(), Dispatcher.THREADS);
  }
}
