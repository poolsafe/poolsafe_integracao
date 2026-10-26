#!/usr/bin/env python3
"""Utilitário independente para geração de relatório Markdown a partir dos registros de ensaio.

Permite analisar os dados de uma execução concluída sem depender de YOLO, OpenCV,
broker RabbitMQ ou câmera. Não calcula precisão/recall/FPR sem referências e
não confunde confiança de detecção com acurácia.
"""
import argparse
import json
from pathlib import Path
import re
import sys
from typing import Dict, Any, Optional


def redact_url(url: str) -> str:
    """Mascara credenciais contidas em URLs (ex.: RTSP)."""
    if not url:
        return ''
    return re.sub(r'(://[^:]+:)[^@]+(@)', r'\1***\2', str(url))


def find_summary_file(path_arg: Optional[str], run_id: Optional[str], data_dir: str, latest: bool) -> Path:
    """Localiza o arquivo resumo.json a partir dos argumentos fornecidos."""
    base_data = Path(data_dir)

    if path_arg:
        p = Path(path_arg)
        if p.is_file():
            return p
        if p.is_dir():
            candidate = p / 'resumo.json'
            if candidate.is_file():
                return candidate
            # Pode ser a pasta raiz de dados com run_id em subpasta
            runs_dir = p / 'runs'
            if runs_dir.is_dir():
                # Tenta latest se houver
                latest_file = p / 'latest_run'
                if latest_file.is_file():
                    rid = latest_file.read_text(encoding='utf-8').strip()
                    c = runs_dir / rid / 'resumo.json'
                    if c.is_file():
                        return c
        raise FileNotFoundError(f"Arquivo de resumo não encontrado no caminho: {path_arg}")

    if latest:
        latest_file = base_data / 'latest_run'
        if latest_file.is_file():
            rid = latest_file.read_text(encoding='utf-8').strip()
            c = base_data / 'runs' / rid / 'resumo.json'
            if c.is_file():
                return c
        # Fallback: pasta mais recente em base_data / 'runs'
        runs_dir = base_data / 'runs'
        if runs_dir.is_dir():
            dirs = [d for d in runs_dir.iterdir() if d.is_dir()]
            dirs.sort(key=lambda d: d.stat().st_mtime, reverse=True)
            for d in dirs:
                c = d / 'resumo.json'
                if c.is_file():
                    return c
        raise FileNotFoundError(f"Nenhuma execução recente encontrada em {base_data}")

    if run_id:
        c = base_data / 'runs' / run_id / 'resumo.json'
        if c.is_file():
            return c
        raise FileNotFoundError(f"Resumo da execução '{run_id}' não encontrado em {c}")

    raise ValueError("Especifique um caminho, --run-id, ou utilize a flag --latest.")


def generate_markdown(data: Dict[str, Any]) -> str:
    """Gera relatório estruturado em formato Markdown a partir do dicionário de resumo."""
    run_id = data.get('run_id', 'desconhecido')
    inicio = data.get('inicio', 'N/D')
    fim = data.get('fim', 'N/D')
    motivo = data.get('motivo_encerramento', 'N/D')
    detalhe_motivo = data.get('detalhe_encerramento', '')
    log_mode = data.get('log_mode', 'N/D')
    source_mode = data.get('source_mode', 'N/D')
    source_safe = redact_url(data.get('source', ''))
    git_rev = data.get('revisao_codigo') or 'Não disponível (sem repositório Git no contêiner)'

    cfg = data.get('configuracao_efetiva', {})
    modelo = data.get('modelo', {})
    classes_modelo = modelo.get('classes', {})
    proc = data.get('processamento', {})
    video = data.get('video')
    eventos = data.get('eventos', {})
    outbox = data.get('outbox', {})

    lines = [
        f"# Relatório de Ensaio — PoolSafe Worker",
        f"",
        f"**Execução (`run_id`):** `{run_id}`  ",
        f"**Modo de Log:** `{log_mode}` | **Origem:** `{source_mode}` | **Revisão Git:** `{git_rev}`",
        f"",
        f"---",
        f"",
        f"## 1. Identificação e Ciclo de Vida",
        f"",
        f"| Campo | Valor |",
        f"|---|---|",
        f"| Início (UTC) | `{inicio}` |",
        f"| Fim (UTC) | `{fim}` |",
        f"| Duração Total | `{proc.get('duracao_total_s', 'N/D')}s` |",
        f"| Motivo de Encerramento | `{motivo}` |",
        f"| Detalhe do Encerramento | {detalhe_motivo or 'Nenhum detalhe adicional'} |",
        f"",
        f"---",
        f"",
        f"## 2. Configuração do Modelo e Incerteza",
        f"",
        f"- **Versão do Modelo:** `{modelo.get('versao', 'N/D')}`",
        f"- **SHA-256 dos Pesos:** `{modelo.get('sha256', 'N/D')}`",
        f"- **Classes Mapeadas no Modelo:** `{classes_modelo}`",
        f"- **Dispositivo de Inferência (Device):** `{cfg.get('device', 'cpu')}`",
        f"- **Resolução (IMGSZ):** `{cfg.get('imgsz', 640)}px` | **Confiança Mínima (CONF):** `{cfg.get('conf', 0.25)}` | **IoU NMS:** `{cfg.get('iou', 0.70)}`",
        f"- **Frames para Alerta:** `{cfg.get('frames_para_alerta', 8)}` frames consecutivos",
        f"- **Frames Normais para Rearme:** `{cfg.get('frames_normais_encerrar', 15)}` frames",
        f"- **Cooldown por Severidade:** `{cfg.get('cooldown_s', 3.0)}s`",
        f"",
        f"---",
        f"",
        f"## 3. Desempenho de Processamento da Entrada",
        f"",
        f"- **Fonte de Entrada:** `{source_safe}`",
    ]

    if video:
        v_fps = video.get('fps')
        v_dur = video.get('duracao_nominal_s')
        v_tot = video.get('total_frames_declarado')
        proc_frames = proc.get('frames_processados', 0)

        lines.extend([
            f"- **FPS Nominal do Vídeo:** `{v_fps if v_fps is not None else 'N/D'}`",
            f"- **Duração Nominal do Vídeo:** `{f'{v_dur:.2f}s' if v_dur is not None else 'N/D'}`",
            f"- **Frames Declarados no Cabeçalho:** `{v_tot if v_tot is not None else 'N/D'}`",
            f"- **Frames Efetivamente Processados:** `{proc_frames}`",
        ])
        if v_tot is not None and v_tot > 0:
            if proc_frames == v_tot:
                lines.append(f"- **Conferência de Leitura:** Leitura completa confirmada ({proc_frames}/{v_tot} frames).")
            else:
                lines.append(f"- **Aviso de Discrepância:** Processados {proc_frames} de {v_tot} frames esperados.")
    else:
        lines.append(f"- **Frames Processados (Stream):** `{proc.get('frames_processados', 'N/D')}`")

    lines.extend([
        f"",
        f"| Métrica de Desempenho | Valor |",
        f"|---|---|",
        f"| Taxa Efetiva de Processamento | `{proc.get('fps_efetivo', 'N/D')} FPS` |",
        f"| Latência do 1º Frame (Warm-up) | `{proc.get('tempo_primeiro_frame_s', 'N/D')}s` |",
        f"| Latência Média Subsequente | `{proc.get('tempo_medio_frames_subsequentes_s', 'N/D')}s` |",
        f"| Latência Mínima por Frame | `{proc.get('tempo_min_frame_s', 'N/D')}s` |",
        f"| Latência Média Global por Frame | `{proc.get('tempo_medio_frame_s', 'N/D')}s` |",
        f"| Latência Máxima por Frame | `{proc.get('tempo_max_frame_s', 'N/D')}s` |",
        f"",
        f"---",
        f"",
        f"## 4. Eventos Confirmados e Supressões",
        f"",
        f"### Contagem por Severidade",
        f"",
        f"| Severidade | Confirmados | Suprimidos por Cooldown |",
        f"|---|---|---|",
        f"| `ATENCAO` | `{eventos.get('confirmados_por_severidade', {}).get('ATENCAO', 0)}` | `{eventos.get('suprimidos_por_severidade', {}).get('ATENCAO', 0)}` |",
        f"| `CRITICA` | `{eventos.get('confirmados_por_severidade', {}).get('CRITICA', 0)}` | `{eventos.get('suprimidos_por_severidade', {}).get('CRITICA', 0)}` |",
        f"",
        f"### Detalhe dos Eventos Locais Confirmados",
        f"",
    ])

    lista_eventos = eventos.get('lista_eventos', [])
    if not lista_eventos:
        lines.append("_Nenhum evento de risco confirmado nesta execução._\n")
    else:
        lines.extend([
            f"| # | Severidade | Classe | Track | Frame | Tempo Vídeo | Confiança | Detected At | Event Key |",
            f"|---|---|---|---|---|---|---|---|---|",
        ])
        for idx, ev in enumerate(lista_eventos, start=1):
            vt = f"{ev.get('video_time_s'):.3f}s" if ev.get('video_time_s') is not None else "N/D"
            conf = f"{ev.get('confidence'):.4f}" if ev.get('confidence') is not None else "N/D"
            lines.append(
                f"| {idx} | `{ev.get('severity')}` | `{ev.get('classe')}` | `{ev.get('track_id')}` | "
                f"`{ev.get('frame_processado')}` | `{vt}` | `{conf}` | `{ev.get('detectedAt')}` | `{ev.get('eventKey')}` |"
            )
        lines.append("")

    lines.extend([
        f"---",
        f"",
        f"## 5. Persistência e Outbox",
        f"",
        f"- **Publicação RabbitMQ Habilitada:** `{cfg.get('publish_rabbit', False)}`",
        f"- **Modo de Teste:** `{cfg.get('test_mode', True)}` | **Publicação em Teste Liberada:** `{cfg.get('allow_test_publish', False)}`",
        f"- **Pendências Finais na Outbox SQLite:** `{outbox.get('pendencias_finais', 0)}`",
        f"",
        f"---",
        f"",
        f"## 6. Limitações e Notas Metodológicas",
        f"",
        f"> [!IMPORTANT]",
        f"> 1. **Confiança não é Acurácia:** O score de confiança emitido pelo modelo YOLO indica a convicção estatística da caixa delimitadora, não a exatidão diagnóstica do evento.",
        f"> 2. **Ausência de Métricas Derivadas sem Ground Truth:** Precisão, revocação (recall) e taxa de falsos positivos (FPR) exigem anotação temporal independente (ground truth) e não são estimadas neste resumo para preservar o rigor científico.",
        f"> 3. **Deduplicação e Cooldown por Câmera:** O cooldown de 3 segundos por severidade opera no escopo global da câmera. Se um segundo indivíduo entrar em risco no mesmo intervalo, seu evento poderá ser suprimido.",
        f"> 4. **Integração Móvel:** Eventos com severidade `ATENCAO` foram classificados e persistidos no worker; a apresentação sem alarme/sirene no aplicativo e nós n8n permanece como pendência das camadas superiores.",
        f""
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Exporta resumo de ensaio do PoolSafe para Markdown.")
    parser.add_argument("path", nargs="?", help="Caminho direto para resumo.json ou pasta do ensaio.")
    parser.add_argument("--run-id", help="Identificador da execução a analisar.")
    parser.add_argument("--data-dir", default="/dados", help="Diretório base de dados (padrão: /dados).")
    parser.add_argument("--latest", action="store_true", help="Gera relatório para a execução mais recente.")
    parser.add_argument("--output", "-o", help="Caminho de saída para o arquivo .md.")
    parser.add_argument("--stdout", action="store_true", help="Exibe o relatório Markdown no console.")

    args = parser.parse_args()

    try:
        summary_file = find_summary_file(args.path, args.run_id, args.data_dir, args.latest)
    except Exception as exc:
        # Fallback local se /dados não existir e data_dir for o padrão
        if args.data_dir == '/dados' and not Path('/dados').exists():
            local_fallback = Path('./dados')
            if local_fallback.exists():
                summary_file = find_summary_file(args.path, args.run_id, str(local_fallback), args.latest)
            else:
                print(f"Erro ao localizar resumo: {exc}", file=sys.stderr)
                sys.exit(1)
        else:
            print(f"Erro ao localizar resumo: {exc}", file=sys.stderr)
            sys.exit(1)

    with open(summary_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    md = generate_markdown(data)

    out_path = args.output
    if not out_path and not args.stdout:
        out_path = summary_file.parent / 'relatorio_ensaio.md'

    if out_path:
        Path(out_path).write_text(md, encoding='utf-8')
        print(f"Relatório Markdown gerado com sucesso em: {out_path}")

    if args.stdout or not out_path:
        print(md)


if __name__ == '__main__':
    main()
