import React from 'react';

type FallbackRender = (args: { error: unknown; reset: () => void }) => React.ReactNode;

export type ErrorBoundaryProps = {
  children: React.ReactNode;
  fallback: React.ReactNode | FallbackRender;
  onError?: (error: unknown) => void;
  onReset?: () => void;
  resetKey?: string | number;
};

type ErrorBoundaryState = {
  error: unknown | null;
  resetKey?: string | number;
};

export class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { error: null, resetKey: this.props.resetKey };

  static getDerivedStateFromError(error: unknown): Partial<ErrorBoundaryState> {
    return { error };
  }

  static getDerivedStateFromProps(
    props: ErrorBoundaryProps,
    state: ErrorBoundaryState,
  ): Partial<ErrorBoundaryState> | null {
    if (props.resetKey !== state.resetKey) {
      return { error: null, resetKey: props.resetKey };
    }
    return null;
  }

  componentDidCatch(error: unknown) {
    this.props.onError?.(error);
  }

  private reset = () => {
    this.setState({ error: null });
    this.props.onReset?.();
  };

  render() {
    if (this.state.error) {
      const { fallback } = this.props;
      if (typeof fallback === 'function') {
        return (fallback as FallbackRender)({ error: this.state.error, reset: this.reset });
      }
      return fallback;
    }
    return this.props.children;
  }
}

