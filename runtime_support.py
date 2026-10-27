def configure_runtime(cfg, cv2, torch):
    torch.set_num_threads(cfg.torch_threads)
    torch.set_num_interop_threads(cfg.torch_interop_threads)
    cv2.setNumThreads(cfg.opencv_threads)
    if str(cfg.device).lower() not in {'cpu', 'mps'} and not torch.cuda.is_available():
        raise RuntimeError('DEVICE solicita CUDA, mas CUDA não está disponível nesta imagem/host')


def synchronize_device(cfg, torch):
    if str(cfg.device).lower() not in {'cpu', 'mps'}:
        torch.cuda.synchronize(cfg.device if str(cfg.device).startswith('cuda') else int(cfg.device))
